const CRC32C_POLY_REVERSED: u32 = 0x82f6_3b78;
const CRC32C_TABLE: [u32; 256] = build_crc32c_table();

pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        let table_index = ((crc ^ u32::from(*byte)) & 0xff) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[table_index];
    }
    !crc
}

#[allow(clippy::cast_possible_truncation)]
const fn build_crc32c_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0;
    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            if crc & 1 == 0 {
                crc >>= 1;
            } else {
                crc = (crc >> 1) ^ CRC32C_POLY_REVERSED;
            }
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}

#[cfg(test)]
mod tests {
    use super::crc32c;

    #[test]
    fn crc32c_matches_standard_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }
}
