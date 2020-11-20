from pathlib import Path

from prefect import Flow, Parameter, unmapped
from prefect.environments import LocalEnvironment
from prefect.environments.storage import Docker
from prefect.engine.executors import LocalDaskExecutor
from prefect.engine.results import LocalResult
from prefect.tasks.secrets import PrefectSecret

import tasks


with Flow(
    name="CommonLit SQL-to-S3",
    storage=Docker(
        registry_url='.../prefect-flows/',
        base_image='.../prefect-flows/prefect:0.13.15-python3.8',
        python_dependencies=list(map(str.strip, (Path(__file__).parent / 'requirements.txt').open().readlines())),
        env_vars={
            'PYTHONPATH': '/opt:${PYTHONPATH}'
        },
        files={
            Path(__file__).parent / 'tasks.py': '/opt/tasks.py'
        }
    ),
    environment=LocalEnvironment(
        executor=LocalDaskExecutor(
            scheduler='threads',
            num_workers=4
        ),
        labels=["cae"]
    ),
    result=LocalResult(dir='./results')
) as flow:
    prefect_secrets = PrefectSecret('COMMON_LIT_SECRETS')
    tables = Parameter('tables', required=True)
    indexed_field = Parameter('indexed_field', default='id', required=True)
    starting_index = Parameter('starting_index', default=0, required=True)
    total_records_to_move = Parameter('total_records_to_move', default=0, required=True)
    number_of_records_in_batch = Parameter('number_of_records_in_batch', default=100000, required=False)

    partitions = tasks.create_data_partitions.map(
        table_name=tables,
        first_index=unmapped(starting_index),
        last_index=unmapped(ending_index),
        secrets=unmapped(prefect_secrets),
        num_of_records_in_batch=unmapped(number_of_records_in_batch)
    )
    flat_partitions = tasks.flatten_nested_list(
        nested_list=partitions
    )
    original_data = tasks.get_data_from_sql.map(
        partition=flat_partitions,
        index=unmapped(indexed_field),
        secrets=unmapped(prefect_secrets)
    )
    raw_table_data = tasks.group_data_partitions_by_table_name(
        data=original_data
    )
    prepared_parquet_files = tasks.prepare_table_data_for_parquet_directory.map(
        grouped_table_data=raw_table_data,
        first_index=unmapped(starting_index),
        num_of_records_in_batch=unmapped(number_of_records_in_batch),
        secrets=unmapped(prefect_secrets)
    )
    parquet_files_to_upload = tasks.flatten_nested_list(
        nested_list=prepared_parquet_files
    )
    transient_downloads = tasks.purge_transient_files.map(
        filename_list=raw_table_data
    )
    transient_downloads.set_upstream(prepared_parquet_files)

    files_to_purge = tasks.identify_s3_files_to_purge.map(
        table_name=tables,
        first_index=unmapped(starting_index),
        secrets=unmapped(prefect_secrets)
    )
    flat_files_to_purge = tasks.flatten_nested_list(
        nested_list=files_to_purge
    )
    cleaned_s3_bucket = tasks.purge_s3_files.map(
        filename=flat_files_to_purge,
        secrets=unmapped(prefect_secrets)
    )
    files_uploaded = tasks.sync_with_s3.map(
        data=parquet_files_to_upload,
        secrets=unmapped(prefect_secrets)
    )
    files_uploaded.set_upstream(cleaned_s3_bucket)

    folder_table_data = tasks.group_data_partitions_by_table_name(
        data=parquet_files_to_upload
    )
    finished = tasks.purge_transient_folders.map(
        filename_list=folder_table_data
    )
    finished.set_upstream(files_uploaded)


if __name__ == "__main__":
    # flow.visualize()
    # flow.register(
    #     project_name="Cisco",
    #     build=True
    # )
    flow.run(
        parameters=dict(
            tables=[
                ## 1st pass
                # 'activities',
                'activity_excerpts',
                ## 2nd pass < 100 MB
                # 'excerpts', 'grades', 'public_schools', 'questions', 'subjects', 'texts',
                # 'schools', 'lesson_events',
                ## 3rd pass < ~1 GB
                # 'roster_grades', 'roster_subjects', 'faculty_memberships',
                # 'notes',
                ## 4th pass < ~5 GB
                # 'annotation_notes', 'lesson_activities', 'lessons', 'student_response_events',
                ## 5th pass > 50 GB
                # 'student_answers',
                # 'student_lessons',
                # 'student_responses',
                #
                # bad indexes
                # 'annotations',
                # no access
                # 'rosters',
            ],
            starting_index=100,
            ending_index=200,
            number_of_records_in_batch=100000,
        ),
        executor=LocalDaskExecutor(
            scheduler="threads"
        ),
        # executor=LocalExecutor(),
        run_on_schedule=False
    )