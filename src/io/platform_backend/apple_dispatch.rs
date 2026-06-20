#![allow(unsafe_code)]

use std::{
    ffi::CString,
    fs::{File, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    ptr::NonNull,
    sync::{Arc, mpsc},
};

use block2::{Block, RcBlock};
use dispatch2::{
    DispatchData, DispatchIO, DispatchIOCloseFlags, DispatchIOStreamType, DispatchQueue,
    DispatchQueueAttr, dispatch_io_handler_t,
};

use crate::{
    error::{Error, Result},
    options::DurabilityMode,
    storage::StorageReadBuffer,
};

enum ReadEvent {
    Chunk(Vec<u8>),
    Done(io::Result<()>),
}

type DispatchIoHandler = Block<dyn Fn(bool, *mut DispatchData, libc::c_int)>;

pub(super) fn read_exact_at_owned(
    path: &Path,
    offset: usize,
    len: usize,
) -> Result<StorageReadBuffer> {
    if len == 0 {
        return Ok(StorageReadBuffer::from_vec(offset, Vec::new()));
    }

    let bytes = read_dispatch(path, platform_offset(offset)?, len)?;
    if bytes.len() != len {
        return Err(Error::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "macOS DispatchIO read returned fewer bytes than requested",
        )));
    }
    Ok(StorageReadBuffer::from_vec(offset, bytes))
}

pub(super) fn read_optional(path: &Path) -> Result<Option<Arc<[u8]>>> {
    match read_dispatch(path, 0, usize::MAX) {
        Ok(bytes) => Ok(Some(Arc::from(bytes))),
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

pub(super) fn write_truncate(path: &Path, bytes: &[u8], durability: DurabilityMode) -> Result<()> {
    write_dispatch(
        path,
        bytes,
        0,
        libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
        durability,
    )
}

pub(super) fn write_existing_or_create(
    path: &Path,
    bytes: &[u8],
    offset: u64,
    durability: DurabilityMode,
) -> Result<()> {
    write_dispatch(
        path,
        bytes,
        offset,
        libc::O_WRONLY | libc::O_CREAT,
        durability,
    )
}

pub(super) fn write_create_new(
    path: &Path,
    bytes: &[u8],
    durability: DurabilityMode,
) -> Result<()> {
    write_dispatch(
        path,
        bytes,
        0,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        durability,
    )
}

pub(super) fn sync_path(path: &Path, durability: DurabilityMode) -> Result<()> {
    if !requires_sync(durability) {
        return Ok(());
    }

    let queue = new_queue();
    let cleanup = RcBlock::new(|_error: libc::c_int| {});
    let dispatch_path = dispatch_path(path)?;
    let path_ptr = NonNull::new(dispatch_path.as_ptr().cast_mut())
        .ok_or_else(|| Error::invalid_options("macOS DispatchIO path is empty"))?;
    let channel = unsafe_channel_with_path(path_ptr, libc::O_RDONLY, 0, &queue, &cleanup);
    let synced = barrier_sync(&channel, durability)?;
    channel.close(DispatchIOCloseFlags(0));
    if !synced {
        sync_path_blocking(path, durability)?;
    }
    Ok(())
}

fn read_dispatch(path: &Path, offset: libc::off_t, len: usize) -> Result<Vec<u8>> {
    let queue = new_queue();
    let cleanup = RcBlock::new(|_error: libc::c_int| {});
    let path = dispatch_path(path)?;
    let path_ptr = NonNull::new(path.as_ptr().cast_mut())
        .ok_or_else(|| Error::invalid_options("macOS DispatchIO path is empty"))?;
    let channel = unsafe_channel_with_path(path_ptr, libc::O_RDONLY, 0, &queue, &cleanup);
    channel.set_high_water(len.clamp(1, 1024 * 1024));

    let (tx, rx) = mpsc::channel();
    let handler: RcBlock<dyn Fn(u8, *mut DispatchData, libc::c_int)> = RcBlock::new(
        move |done: u8, data: *mut DispatchData, error: libc::c_int| {
            if !data.is_null() {
                // SAFETY: DispatchIO provides a valid `dispatch_data_t` pointer
                // only for the duration of this callback. We immediately copy
                // the bytes into an owned `Vec` and do not retain the pointer.
                let data = unsafe { &*data };
                let _ = tx.send(ReadEvent::Chunk(data.to_vec()));
            }
            if done != 0 {
                let result = if error == 0 {
                    Ok(())
                } else {
                    Err(io::Error::from_raw_os_error(error))
                };
                let _ = tx.send(ReadEvent::Done(result));
            }
        },
    );

    let handler_ptr = io_handler_ptr(&handler);
    // SAFETY: `handler_ptr` is the same block pointer with the DispatchIO
    // handler ABI shape (see `io_handler_ptr`). `channel`, `queue`, and the
    // heap-allocated handler remain alive until the done event arrives.
    unsafe {
        channel.read(offset, len, &queue, handler_ptr);
    }

    let mut bytes = Vec::new();
    loop {
        match rx.recv() {
            Ok(ReadEvent::Chunk(chunk)) => bytes.extend_from_slice(&chunk),
            Ok(ReadEvent::Done(Ok(()))) => break,
            Ok(ReadEvent::Done(Err(error))) => {
                channel.close(DispatchIOCloseFlags::DISPATCH_IO_STOP);
                return Err(Error::Io(error));
            }
            Err(_) => {
                channel.close(DispatchIOCloseFlags::DISPATCH_IO_STOP);
                return Err(Error::runtime_busy("macOS DispatchIO read channel closed"));
            }
        }
    }

    channel.close(DispatchIOCloseFlags(0));
    Ok(bytes)
}

fn write_dispatch(
    path: &Path,
    bytes: &[u8],
    offset: u64,
    flags: libc::c_int,
    durability: DurabilityMode,
) -> Result<()> {
    let queue = new_queue();
    let cleanup = RcBlock::new(|_error: libc::c_int| {});
    let dispatch_path = dispatch_path(path)?;
    let path_ptr = NonNull::new(dispatch_path.as_ptr().cast_mut())
        .ok_or_else(|| Error::invalid_options("macOS DispatchIO path is empty"))?;
    let channel = unsafe_channel_with_path(path_ptr, flags, 0o666, &queue, &cleanup);
    channel.set_high_water(bytes.len().clamp(1, 1024 * 1024));

    let dispatch_data = if bytes.is_empty() {
        None
    } else {
        Some(DispatchData::from_bytes(bytes))
    };
    let data: &DispatchData = match dispatch_data.as_deref() {
        Some(data) => data,
        None => DispatchData::empty(),
    };
    let (tx, rx) = mpsc::channel();
    let handler: RcBlock<dyn Fn(u8, *mut DispatchData, libc::c_int)> = RcBlock::new(
        move |done: u8, _data: *mut DispatchData, error: libc::c_int| {
            if done != 0 {
                let result = if error == 0 {
                    Ok(())
                } else {
                    Err(io::Error::from_raw_os_error(error))
                };
                let _ = tx.send(result);
            }
        },
    );

    let offset = libc::off_t::try_from(offset)
        .map_err(|_| Error::invalid_options("macOS DispatchIO write offset overflow"))?;
    let handler_ptr = io_handler_ptr(&handler);
    // SAFETY: `handler_ptr` is the same block pointer with the DispatchIO
    // handler ABI shape (see `io_handler_ptr`). `channel`, `queue`, `data`, and
    // the heap-allocated handler remain alive until the completion event.
    unsafe {
        channel.write(offset, data, &queue, handler_ptr);
    }

    match rx.recv() {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            channel.close(DispatchIOCloseFlags::DISPATCH_IO_STOP);
            if should_retry_with_blocking_write(&error, flags) {
                return write_path_blocking(path, bytes, offset, flags, durability);
            }
            return Err(Error::Io(error));
        }
        Err(_) => {
            channel.close(DispatchIOCloseFlags::DISPATCH_IO_STOP);
            return Err(Error::runtime_busy("macOS DispatchIO write channel closed"));
        }
    }

    let synced = barrier_sync(&channel, durability)?;
    channel.close(DispatchIOCloseFlags(0));
    if flags & libc::O_CREAT != 0 && !path.exists() {
        return write_path_blocking(path, bytes, offset, flags, durability);
    }
    if !synced {
        sync_path_blocking(path, durability)?;
    }
    Ok(())
}

fn barrier_sync(channel: &DispatchIO, durability: DurabilityMode) -> Result<bool> {
    if !requires_sync(durability) {
        return Ok(true);
    }

    let fd = channel.descriptor();
    if fd < 0 {
        return Ok(false);
    }

    let (tx, rx) = mpsc::channel();
    let block: RcBlock<dyn Fn()> = RcBlock::new(move || {
        // Strict modes flush the drive cache with F_FULLFSYNC; non-strict modes
        // use a plain fsync. The single source of truth for that decision lives
        // in `crate::durability` (see `sync_file_for_durability`).
        let result = crate::durability::sync_fd_for_durability(fd, durability);
        let _ = tx.send(result);
    });
    let block_ptr = RcBlock::as_ptr(&block);
    // SAFETY: `block` stays alive until the barrier sends its completion.
    unsafe {
        channel.barrier(block_ptr);
    }

    match rx.recv() {
        Ok(Ok(())) => Ok(true),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(Error::runtime_busy(
            "macOS DispatchIO barrier channel closed",
        )),
    }
}

fn sync_path_blocking(path: &Path, durability: DurabilityMode) -> Result<()> {
    if !requires_sync(durability) {
        return Ok(());
    }

    let file = File::open(path).map_err(|error| {
        Error::Io(io::Error::new(
            error.kind(),
            format!(
                "macOS DispatchIO sync fallback failed to open {}: {error}",
                path.display()
            ),
        ))
    })?;
    crate::durability::sync_file_for_durability(&file, durability)
}

fn write_path_blocking(
    path: &Path,
    bytes: &[u8],
    offset: libc::off_t,
    flags: libc::c_int,
    durability: DurabilityMode,
) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true);
    if flags & libc::O_EXCL != 0 {
        options.create_new(true);
    } else if flags & libc::O_CREAT != 0 {
        options.create(true);
    }
    if flags & libc::O_TRUNC != 0 {
        options.truncate(true);
    }

    let mut file = options.open(path).map_err(|error| {
        Error::Io(io::Error::new(
            error.kind(),
            format!(
                "macOS DispatchIO write fallback failed to open {}: {error}",
                path.display()
            ),
        ))
    })?;
    if offset > 0 {
        file.seek(SeekFrom::Start(u64::try_from(offset).map_err(|_| {
            Error::invalid_options("macOS DispatchIO write fallback offset overflow")
        })?))?;
    }
    file.write_all(bytes).map_err(|error| {
        Error::Io(io::Error::new(
            error.kind(),
            format!(
                "macOS DispatchIO write fallback failed to write {}: {error}",
                path.display()
            ),
        ))
    })?;
    persist_file_blocking(&file, durability)
}

fn persist_file_blocking(file: &File, durability: DurabilityMode) -> Result<()> {
    crate::durability::sync_file_for_durability(file, durability)
}

fn should_retry_with_blocking_write(error: &io::Error, flags: libc::c_int) -> bool {
    flags & libc::O_CREAT != 0 && error.kind() != io::ErrorKind::AlreadyExists
}

fn unsafe_channel_with_path(
    path: NonNull<libc::c_char>,
    flags: libc::c_int,
    mode: libc::mode_t,
    queue: &DispatchQueue,
    cleanup: &block2::DynBlock<dyn Fn(libc::c_int)>,
) -> dispatch2::DispatchRetained<DispatchIO> {
    // SAFETY: `path` points to a NUL-terminated absolute path for the duration
    // of this call. DispatchIO owns the opened descriptor once the first
    // operation starts and reports open errors through the I/O handler.
    unsafe {
        DispatchIO::with_path(
            DispatchIOStreamType::DISPATCH_IO_RANDOM,
            path,
            flags,
            mode,
            queue,
            cleanup,
        )
    }
}

fn io_handler_ptr(
    handler: &RcBlock<dyn Fn(u8, *mut DispatchData, libc::c_int)>,
) -> dispatch_io_handler_t {
    // Apple encodes the dispatch `done` flag as a C Boolean. `block2` does not
    // currently provide an ObjC encoding for Rust `bool`, so this mirrors
    // dispatch2's own `DispatchData::to_vec` pattern and uses `u8` at the Rust
    // closure boundary. The block remains a three-argument DispatchIO handler:
    // boolean byte, dispatch data pointer, and errno-style integer.
    RcBlock::as_ptr(handler).cast::<DispatchIoHandler>()
}

fn dispatch_path(path: &Path) -> Result<CString> {
    let path = absolute_path(path)?;
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| Error::invalid_options("macOS DispatchIO path contains a NUL byte"))
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir().map_err(Error::Io)?.join(path))
    }
}

fn new_queue() -> dispatch2::DispatchRetained<DispatchQueue> {
    DispatchQueue::new(
        "io.trine.platform.apple-dispatch",
        DispatchQueueAttr::SERIAL,
    )
}

fn requires_sync(durability: DurabilityMode) -> bool {
    crate::durability::requires_file_sync(durability)
}

fn platform_offset(offset: usize) -> Result<libc::off_t> {
    libc::off_t::try_from(offset)
        .map_err(|_| Error::invalid_options("macOS DispatchIO read offset overflow"))
}
