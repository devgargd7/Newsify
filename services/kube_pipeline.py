import kfp
from kfp import dsl


@dsl.pipeline(name="Retraining Pipeline")
def retrain_pipeline():
    train_op = dsl.ContainerOp(
        name="train",
        image="your-registry/recommendation-trainer:latest",
        command=["python", "recommendation_trainer.py"]
    )

kfp.Client().create_run_from_pipeline_func(retrain_pipeline)