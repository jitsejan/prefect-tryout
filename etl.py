from prefect import context, task, Flow

@task
def extract_reference_data():
    # fetch reference data
    reference_data = 23
    return reference_data

@task
def extract_live_data():
    live_data = 10
    return live_data

@task
def transform(live_data, reference_data):
    transformed_data = live_data + reference_data
    return transformed_data

@task
def load_reference_data(reference_data):
    logger = context.get("logger")
    logger.info(f"Reference data: {reference_data}")

@task
def load_live_data(transformed_data):
    logger = context.get("logger")
    logger.info(f"Transformed data: {transformed_data}")


with Flow("Sample-ETL") as flow:
    reference_data = extract_reference_data()
    live_data = extract_live_data()

    transformed_live_data = transform(live_data, reference_data)

    load_reference_data(reference_data)
    load_live_data(transformed_live_data)

    flow.run()
