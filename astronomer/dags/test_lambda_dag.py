from airflow.sdk import dag
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

AWS_CONN_ID = "dna_int_aws_conn"

@dag(
    tags=["DNA", "TEST DAG"],
)
def lambda_test_dag():
    invoke_lambda = LambdaInvokeFunctionOperator(
        task_id="lambda_connectivity_test",
        aws_conn_id=AWS_CONN_ID,
        function_name="ttdna-int-astro-dummy-lambda",
        invocation_type="RequestResponse",
        log_type="Tail",
        payload='{"key1": "value1", "key2": "value2"}',
    )

lambda_test_dag()
