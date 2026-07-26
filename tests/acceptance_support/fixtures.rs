pub(crate) fn parse_rows(specification: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    if specification.is_empty() {
        return Vec::new();
    }
    specification
        .split(',')
        .map(|entry| {
            let (key, value) = entry
                .split_once('=')
                .expect("row fixture uses key=value syntax");
            (key.as_bytes().to_vec(), value.as_bytes().to_vec())
        })
        .collect()
}
