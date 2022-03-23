import prefect
from prefect import task, Flow, Parameter

@task
def extract_reference_data(name):
    reference_data = len(name) * 23
    return reference_data

@task
def extract_live_data():
    live_data = 10
    return live_data

@task
def transform(live_data, reference_data, multiplier):
    transformed_data = (live_data + reference_data) * multiplier
    return transformed_data

@task
def load_reference_data(reference_data):
    logger = prefect.context.get("logger")
    logger.info(f"Reference data: {reference_data}")

@task
def load_live_data(transformed_data):
    logger = prefect.context.get("logger")
    logger.info(f"Transformed data: {transformed_data}")


with Flow("Sample-ETL") as flow:
    multiplier = Parameter("multiplier", default=1)
    name = Parameter("name", default="etl-name")
    reference_data = extract_reference_data(name=name)
    live_data = extract_live_data()

    transformed_live_data = transform(live_data, reference_data, multiplier)

    load_reference_data(reference_data)
    load_live_data(transformed_live_data)

parameters = {
    'multiplier': 10,
    'name': 'sample-etl'
}
flow.run()
flow.run(multiplier=10)
flow.run(parameters)
