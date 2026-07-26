mod acceptance_support;

use acceptance_support::TrineWorld;
use cucumber::World as _;
use futures::FutureExt as _;

async fn run_acceptance() {
    TrineWorld::cucumber()
        .after(|_, _, _, _, world| {
            async move {
                if let Some(world) = world {
                    world
                        .cleanup()
                        .await
                        .expect("acceptance scenario storage is completely cleaned");
                }
            }
            .boxed_local()
        })
        .with_default_cli()
        .run_and_exit("tests/features")
        .await;
}

#[cfg(feature = "s3")]
#[tokio::main(flavor = "multi_thread")]
async fn main() {
    run_acceptance().await;
}

#[cfg(not(feature = "s3"))]
fn main() {
    futures::executor::block_on(run_acceptance());
}
