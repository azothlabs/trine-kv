mod acceptance_support;

use acceptance_support::TrineWorld;
use cucumber::World as _;
use futures::FutureExt as _;

fn main() {
    futures::executor::block_on(async {
        TrineWorld::cucumber()
            .after(|_, _, _, _, world| {
                async move {
                    if let Some(world) = world {
                        world
                            .cleanup()
                            .await
                            .expect("native acceptance storage is completely cleaned");
                    }
                }
                .boxed_local()
            })
            .with_default_cli()
            .run_and_exit("tests/features_native")
            .await;
    });
}
