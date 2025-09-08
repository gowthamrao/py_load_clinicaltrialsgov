import structlog
from typing import Dict, List, Any, Literal

from py_load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface
from py_load_clinicaltrialsgov.extractor.api_client import APIClient
from py_load_clinicaltrialsgov.transformer.transformer import Transformer

logger = structlog.get_logger(__name__)


class Orchestrator:
    """
    Orchestrates the ETL process from extraction to loading.
    """

    TABLE_METADATA: Dict[str, List[str]] = {
        "raw_studies": ["nct_id"],
        "studies": ["nct_id"],
        "sponsors": ["nct_id", "name", "agency_class"],
        "conditions": ["nct_id", "name"],
        "interventions": ["nct_id", "intervention_type", "name"],
        "design_outcomes": ["nct_id", "outcome_type", "measure"],
    }

    def __init__(
        self,
        connector: DatabaseConnectorInterface,
        api_client: APIClient,
        transformer: Transformer,
    ) -> None:
        self.connector = connector
        self.api_client = api_client
        self.transformer = transformer

    def run_etl(self, load_type: str) -> None:
        """
        Executes the full ETL pipeline.
        """
        log = logger.bind(load_type=load_type)
        log.info("etl_process_started")

        try:
            self.connector.manage_transaction("begin")

            updated_since = None
            if load_type == "delta":
                updated_since = self.connector.get_last_successful_load_timestamp()
                if updated_since:
                    log.info(
                        "delta_load_initiated",
                        updated_since=updated_since.isoformat(),
                    )
                else:
                    log.info("no_successful_load_found_performing_full_load")

            studies_iterator = self.api_client.get_all_studies(
                updated_since=updated_since
            )

            record_count = 0
            for study in studies_iterator:
                nct_id = (
                    study.protocol_section.identification_module.get("nctId")
                    if study.protocol_section.identification_module
                    else None
                )
                try:
                    self.transformer.transform_study(study)
                    record_count += 1
                    if record_count % 100 == 0:
                        log.info("processed_studies_batch", record_count=record_count)
                except Exception as e:
                    log.error(
                        "study_transformation_failed",
                        nct_id=nct_id,
                        error=str(e),
                        exc_info=True,
                    )
                    if nct_id:
                        self.connector.record_failed_study(
                            nct_id=nct_id,
                            payload=study.model_dump(),
                            error_message=str(e),
                        )

            log.info("finished_processing_studies", total_record_count=record_count)

            dataframes = self.transformer.get_dataframes()
            for table_name, df in dataframes.items():
                if not df.empty:
                    log.info(
                        "loading_data_into_table",
                        table_name=table_name,
                        record_count=len(df),
                    )
                    primary_keys = self.TABLE_METADATA.get(table_name)
                    if not primary_keys:
                        log.error(
                            "no_primary_key_defined_for_table", table_name=table_name
                        )
                        continue

                    self.connector.bulk_load_staging(table_name, df)

                    # Parent tables use 'upsert', child tables use 'delete_insert'
                    # to ensure the full set of child records is replaced.
                    is_parent_table = primary_keys == ["nct_id"]
                    strategy: Literal[
                        "upsert", "delete_insert"
                    ] = "upsert" if is_parent_table else "delete_insert"

                    self.connector.execute_merge(table_name, primary_keys, strategy)

            metrics: Dict[str, Any] = {"records_processed": record_count}
            self.connector.record_load_history("SUCCESS", metrics)
            self.connector.manage_transaction("commit")
            log.info("etl_process_completed_successfully", metrics=metrics)

        except Exception as e:
            log.error("etl_process_failed", error=str(e), exc_info=True)
            self.connector.manage_transaction("rollback")
            metrics = {"error": str(e)}
            self.connector.record_load_history("FAILURE", metrics)
        finally:
            self.api_client.close()
