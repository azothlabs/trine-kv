use cucumber::{given, then, when};
use trine_kv::{Iter, KeyRange};

use super::{fixtures::parse_rows, world::TrineWorld};

async fn drain(mut cursor: Iter) -> trine_kv::Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut rows = Vec::new();
    while let Some(row) = cursor.next().await? {
        rows.push((row.key, row.value));
    }
    Ok(rows)
}

#[given(expr = "keys {string} exist")]
async fn keys_exist(world: &mut TrineWorld, specification: String) {
    for (key, value) in parse_rows(&specification) {
        world
            .db()
            .put(key, value)
            .await
            .expect("fixture write commits");
    }
}

#[when("I scan all keys forward")]
async fn scan_all_forward(world: &mut TrineWorld) {
    let cursor = world
        .db()
        .range(&KeyRange::all())
        .await
        .expect("forward cursor opens");
    world.rows = drain(cursor).await.expect("forward cursor drains");
}

#[when("I scan all keys in reverse")]
async fn scan_all_reverse(world: &mut TrineWorld) {
    let cursor = world
        .db()
        .range_reverse(&KeyRange::all())
        .await
        .expect("reverse cursor opens");
    world.rows = drain(cursor).await.expect("reverse cursor drains");
}

#[when(expr = "I scan keys from {string} up to {string}")]
async fn scan_half_open(world: &mut TrineWorld, start: String, end: String) {
    let cursor = world
        .db()
        .range(&KeyRange::half_open(start.into_bytes(), end.into_bytes()))
        .await
        .expect("bounded cursor opens");
    world.rows = drain(cursor).await.expect("bounded cursor drains");
}

#[when(expr = "I scan keys with prefix {string}")]
async fn scan_prefix(world: &mut TrineWorld, prefix: String) {
    let cursor = world
        .db()
        .prefix(prefix.into_bytes())
        .await
        .expect("prefix cursor opens");
    world.rows = drain(cursor).await.expect("prefix cursor drains");
}

#[given("I create a forward cursor over all keys")]
async fn retain_forward_cursor(world: &mut TrineWorld) {
    world.retained_cursor = Some(
        world
            .db()
            .range(&KeyRange::all())
            .await
            .expect("retained cursor opens"),
    );
}

#[when("I drain the retained cursor")]
async fn drain_retained_cursor(world: &mut TrineWorld) {
    let cursor = world
        .retained_cursor
        .take()
        .expect("forward cursor was retained");
    world.rows = drain(cursor).await.expect("retained cursor drains");
}

#[then(expr = "the rows are {string}")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber expression captures require an owned String parameter"
)]
fn rows_are(world: &mut TrineWorld, expected: String) {
    assert_eq!(world.rows, parse_rows(&expected));
}
