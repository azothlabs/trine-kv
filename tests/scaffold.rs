use trine_kv::{
    CompressionProfile, Db, Direction, KeyRange, PrefixExtractor, Sequence, WriteBatch,
    codec::{BlockCodec, CodecId, FastLz4BlockCodec, NoneCodec},
};

#[test]
fn scaffold_exposes_v1_public_boundaries() {
    let db = Db::open_memory_sync().expect("memory db scaffold opens");
    db.put_sync(b"default-key", b"default-value")
        .expect("default bucket put works");

    assert_eq!(
        db.get_sync(b"default-key")
            .expect("default bucket get works"),
        Some(b"default-value".to_vec())
    );
    assert_eq!(db.snapshot().read_sequence(), Sequence::new(1));
    assert_eq!(db.stats().live_buckets, 1);
    assert_eq!(CompressionProfile::Fast.codec_id(), CodecId::FastLz4Block);

    let mut batch = WriteBatch::new();
    batch.put(b"a", b"b");
    batch.delete_range(KeyRange::half_open(b"a", b"z"));
    assert_eq!(batch.len(), 2);

    let iter = trine_kv::Bucket::empty_iter(Direction::Forward);
    assert_eq!(iter.direction(), Direction::Forward);
}

#[test]
fn prefix_and_none_codec_scaffold_are_usable() {
    let extractor = PrefixExtractor::Separator(b':');
    assert_eq!(extractor.extract(b"user:42"), Some(&b"user:"[..]));

    let codec = NoneCodec;
    let encoded = codec.encode(b"plain block").expect("none codec encodes");
    let decoded = codec
        .decode(&encoded, "plain block".len())
        .expect("none codec decodes");
    assert_eq!(decoded, b"plain block");

    let fast_codec = FastLz4BlockCodec;
    let encoded = fast_codec
        .encode(b"fast block fast block fast block")
        .expect("lz4 codec encodes");
    let decoded = fast_codec
        .decode(&encoded, "fast block fast block fast block".len())
        .expect("lz4 codec decodes");
    assert_eq!(decoded, b"fast block fast block fast block");
}
