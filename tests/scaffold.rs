use trine_kv::{
    CompressionProfile, Db, DbOptions, Direction, KeyRange, KeyspaceOptions, PrefixExtractor,
    Sequence, WriteBatch,
    codec::{BlockCodec, CodecId, NoneCodec},
};

#[test]
fn scaffold_exposes_v1_public_boundaries() {
    let db = Db::memory(DbOptions::memory()).expect("memory db scaffold opens");
    let keyspace = db
        .keyspace("default", KeyspaceOptions::default())
        .expect("keyspace scaffold opens");

    assert_eq!(keyspace.name().as_str(), "default");
    assert_eq!(db.snapshot().read_sequence(), Sequence::ZERO);
    assert_eq!(db.stats().live_keyspaces, 1);
    assert_eq!(CompressionProfile::Fast.codec_id(), CodecId::FastLz4Block);

    let mut batch = WriteBatch::new();
    batch.insert("default", b"a", b"b");
    batch.remove_range("default", KeyRange::half_open(b"a", b"z"));
    assert_eq!(batch.len(), 2);

    let iter = trine_kv::Keyspace::empty_iter(Direction::Forward);
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
}
