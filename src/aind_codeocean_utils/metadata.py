from datetime import datetime
import os

import mlflow
from codeocean import CodeOcean
import mlflow


import aind_data_schema.core.model as ms
import aind_data_access_api.document_db as docdb

API_GATEWAY_HOST = "api.allenneuraldynamics.org"
DATABASE = "metadata_index"
COLLECTION = "data_assets"

def get_docdb_client() -> docdb.MetadataDbClient:
    return docdb.MetadataDbClient(
        host=API_GATEWAY_HOST,
        database=DATABASE,
        collection=COLLECTION,
    )

AIND_MODEL_LICENSE = "CC-BY-4.0"

def _create_base_model() -> ms.Model:
    return ms.Model.model_construct(
        license=AIND_MODEL_LICENSE,
        developer_institution=ms.Organization.AIND,

    )

def get_codeocean_client() -> CodeOcean:
    url = os.getenv("MLFLOW_TRACKING_URI")
    domain = url[:url.find("/mlflow-server")]
    client = CodeOcean(domain=domain, token=os.getenv("API_SECRET"))
    return client

def get_codeocean_inputs(client: CodeOcean) -> List[str]:
    computation = client.computations.get_computation(os.getenv("CO_COMPUTATION_ID"))
    mounted_data_ids = [data.id for data in computation.mounted_data]
    client = get_docdb_client()
    mounted_metadata = client.retrieve_docdb_records(
        filter_query={"external_links.Code Ocean": {"$in": mounted_data_ids}},
        projection={"location": 1, "name": 1},
    )
    return [data['location'] for data in mounted_metadata]



def get_mlflow_context():
    # get current mlflow run
    run = mlflow.active_run()
    if run is None:
        raise ValueError("No active mlflow run found")
    data = run.data
    extra_tags = {k: v for k, v in data.tags.items() if not k.startswith("mlflow.")}
    models = data.tags["mlflow.log-model.history"]
    if len(models) == 0:
        raise ValueError("No models logged in run")
    if len(models) > 1:
        raise ValueError("Multiple models logged in single run")
    
    model = models[0]
    if "sklearn" in model["flavors"]:
        details =  model["flavors"]["sklearn"]
        arch = ms.ModelArchitecture(
            backbone=ms.ModelBackbone.CUSTOM,
            software=[
                ms.Software(
                    name="scikit-learn",
                    version=details["sklearn-version"],
                )
            ],
            notes="From mlflow sklearn integration",
        )
    elif "python_function" in model["flavors"]:
        details = model["flavors"]["python_function"]
        arch = ms.ModelArchitecture(
            backbone=ms.ModelBackbone.CUSTOM,
            notes="From mlflow integration",
        )
    else:
        raise ValueError("Model flavor not supported")
    
    run_info = run.info
    start_time = datetime.fromtimestamp(run_info.start_time / 1000)
    end_time = datetime.fromtimestamp(run_info.end_time / 1000)
    metrics = [ms.PerformanceMetric(name=name, value=value) for name, value in run.data.metrics.items()]
    training = ms.ModelTraining(
        input_location=get_codeocean_inputs(get_codeocean_client()),
        # output_location=model["artifact_path"],
        # code_url=model["source"],
        # software_version=model["flavors"]["python_function"]["python_version"],
        start_date_time=start_time,
        end_date_time=end_time,
        train_performance=metrics,
        parameters=run.params,

    )
    return training, arch

    # m = ms.Model(
    #     name="2024_01_01_ResNet18_SmartSPIM.h5",
    #     developer_full_name=["Joe Schmoe"],
    #     modality=[ms.Modality.SPIM],