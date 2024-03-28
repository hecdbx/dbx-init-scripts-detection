import base64
from typing import List
import json
import os
import re
import functools as ft
import logging as log

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails
import databricks.sdk.errors.mapping as sdk_errors
from databricks.sdk.errors.base import DatabricksError


job_cluster_re = re.compile(r'^job-[0-9]+-run-[0-9]+$')


def is_job_cluster(name: str) -> bool:
    return job_cluster_re.fullmatch(name) is not None


def _get_dbfs_init_scripts(cl: ClusterDetails):
    """
    Extracts list of init scripts on DBFS from the cluster details object
    :param cl: cluster object
    :return: list of init scripts on DBFS used by this cluster
    """
    scripts = []
    for script in (cl.init_scripts or []):
        if script.dbfs:
            scripts.append(script.dbfs.destination)

    return scripts


def identify_clusters_or_jobs_with_named_scripts(wc: WorkspaceClient):
    """
    Identifies a list of clusters with legacy named init scripts on DBFS.
    """
    dbfs_scripts = {}
    flag = "enableDeprecatedClusterNamedInitScripts"
    named_init_script_path = "dbfs:/databricks/init/"
    res = wc.api_client.do(path=f'/api/2.0/workspace-conf?keys={flag}', method="GET")
    flag_val = res.get(flag, 'false')
    if flag_val is not None and flag_val == 'false':
        log.info("enableDeprecatedClusterNamedInitScripts isn't enabled")
        return dbfs_scripts

    try:
        lst = wc.dbfs.list(named_init_script_path)
        for f in lst:
            if not f.is_dir:
                continue
            try:
                tl = wc.dbfs.list(f.path)
                scripts = [file.path for file in tl if not file.is_dir]
                if scripts:
                    nm = f.path.split("/")[-1]
                    log.info(f"Found {len(scripts)} cluster-named init scripts in directory '{f.path}': {scripts}")
                    dbfs_scripts[nm] = {"scripts": scripts}
            except sdk_errors.NotFound:
                log.debug(f"Error reading {f.path}")
    except sdk_errors.NotFound:
        log.debug(f"Error reading {named_init_script_path}")
        return dbfs_scripts

    return dbfs_scripts


def identify_legacy_global_init_scripts(wc: WorkspaceClient):
    """
    Identifies a list of legacy global init scripts on DBFS.
    """
    dbfs_scripts = []
    flag = "enableDeprecatedGlobalInitScripts"
    named_init_script_path = "dbfs:/databricks/init/"
    res = wc.api_client.do(path=f'/api/2.0/workspace-conf?keys={flag}', method="GET")
    flag_val = res.get(flag)
    if flag_val is not None and flag_val == 'false':
        log.info("enableDeprecatedGlobalInitScripts isn't enabled")
        return dbfs_scripts
    try:
        lst = wc.dbfs.list(named_init_script_path)
        return [file.path for file in lst if not file.is_dir]
    except sdk_errors.NotFound as ex:
        log.debug(f"Error reading {named_init_script_path}")

    return dbfs_scripts


def identify_clusters(wc: WorkspaceClient):
    """
    Analyzes interactive clusters in a given Databricks workspace and identify clusters having
    init scripts on DBFS.
    :param wc: Databricks workspace client
    :return: a map from cluster ID to an object containing cluster name and list of init scripts on DBFS
    """
    dbfs_scripts = {}
    clusters = list(wc.clusters.list())
    log.info(f"Processing {len(clusters)} clusters")
    for cl in clusters:
        if is_job_cluster(cl.cluster_name):
            continue
        scripts = _get_dbfs_init_scripts(cl)
        if not scripts:
            continue

        cluster_id = cl.cluster_id
        cluster_name = cl.cluster_name
        log.info(
            f"Found {len(scripts)} DBFS init scripts in cluster '{cluster_name}': {scripts}")
        dbfs_scripts[cluster_id] = {
            "name": cluster_name,
            "scripts": scripts,
        }

    return dbfs_scripts


def identify_jobs(wc: WorkspaceClient):
    """
    Analyzes Databricks jobs in a given Databricks workspace and identify jobs having
    init scripts on DBFS.
    :param wc: Databricks workspace client
    :return: a map from Job ID to the information about jobs clusters & tasks that are using init scripts on DBFS
    """
    dbfs_scripts = {}
    jobs = list(wc.jobs.list(expand_tasks=True, limit=100))
    log.info(f"Processing {len(jobs)} jobs")
    for job in jobs:
        job_scripts = {}
        job_clusters = {}
        for jcl in (job.settings.job_clusters or []):
            scripts = _get_dbfs_init_scripts(jcl.new_cluster)
            if scripts:
                job_clusters[jcl.job_cluster_key] = scripts

        for task in (job.settings.tasks or []):
            new_clusters_scripts = []
            job_clusters_scripts = {}
            if task.new_cluster:
                new_clusters_scripts = _get_dbfs_init_scripts(task.new_cluster)
            elif task.job_cluster_key and task.job_cluster_key in job_clusters:
                job_clusters_scripts[task.job_cluster_key] = job_clusters[task.job_cluster_key]
            for s in new_clusters_scripts + ft.reduce(lambda x, y: x + y,
                                                      job_clusters_scripts.values(), []):
                if s not in job_scripts:
                    job_scripts[s] = {"new_clusters": [], "job_clusters": {}}
            for s in new_clusters_scripts:
                job_scripts[s]["new_clusters"].append(task.task_key)
            for jc, scripts in job_clusters_scripts.items():
                for s in scripts:
                    if jc not in job_scripts[s]["job_clusters"]:
                        job_scripts[s]["job_clusters"][jc] = []
                    job_scripts[s]["job_clusters"][jc].append(task.task_key)

        if job_scripts:
            log.info(
                f"Found {len(job_scripts)} DBFS init scripts in job '{job.settings.name}': {job_scripts}")
            dbfs_scripts[job.job_id] = {
                "job_id": job.job_id,
                "name": job.settings.name,
                "scripts": job_scripts,
            }

    return dbfs_scripts


def identify_dlt_pipelines(wc: WorkspaceClient):
    """
    Analyzes Delta Live Table pipelines in a given Databricks workspace and identify pipelines
    having init scripts on DBFS.
    :param wc: Databricks workspace client
    :return: a map from DLT Pipeline ID to the object with name and list of init scripts on DBFS
    """
    dbfs_scripts = {}
    pipelines = list(wc.pipelines.list_pipelines())
    log.info(f"Processing {len(pipelines)} DLT pipelines")
    i = 0
    for p in pipelines:
        # We need this because SDK doesn't have support for init scripts
        dlt = wc.api_client.do("GET", "/api/2.0/pipelines/" + p.pipeline_id)
        dlt_scripts = {}
        for cl in dlt['spec'].get('clusters', []):
            cluster = cl.get('label', 'default')
            for init in cl.get('init_scripts', []):
                if 'dbfs' in init:
                    script = init['dbfs']['destination']
                    if script not in dlt_scripts:
                        dlt_scripts[script] = []
                    dlt_scripts[script].append(cluster)
        if dlt_scripts:
            log.info(
                f"Found {len(dlt_scripts)} DBFS init scripts in DLT '{dlt['spec']['name']}': {dlt_scripts}")
            dbfs_scripts[p.pipeline_id] = {
                "pipeline_id": p.pipeline_id,
                "name": dlt['spec']['name'],
                "scripts": dlt_scripts,
            }
        i = i + 1
        if (i % 50) == 0:
            log.info(f"Processed {i} DLT pipelines out of {len(pipelines)}")

    return dbfs_scripts


def identify_cluster_policies(wc: WorkspaceClient):
    """
    Analyzes Databricks cluster policies in a given Databricks workspace and identify cluster policies
    referring init scripts on DBFS.
    :param wc: Databricks workspace client
    :return: a map from cluster policy ID to the object with cluster policy name and list of init scripts on DBFS
    """
    dbfs_scripts = {}
    policies = list(wc.cluster_policies.list())
    log.info(f"Processing {len(policies)} cluster policies")
    for policy in policies:
        scripts = []
        d = json.loads(policy.definition)
        for k, v in d.items():
            if not isinstance(v, dict):
                continue
            typ = v.get("type")
            if not typ:
                continue
            val = v.get("value") or v.get("defaultValue")
            if not val:
                continue
            if typ == "fixed" and k.startswith("init_scripts.") and k.endswith(".dbfs.destination"):
                scripts.append(val)
        if scripts:
            log.info(
                f"Found {len(scripts)} DBFS init scripts in cluster policy '{policy.name}': {scripts}")
            dbfs_scripts[policy.policy_id] = {
                "name": policy.name,
                "scripts": scripts,
            }

    return dbfs_scripts


def extract_init_scripts(clusters: dict = None, jobs: dict = None, dlts: dict = None,
                         policies: dict = None, clusters_with_named: dict = None):
    """
    Generates a list of uniq init scripts from set of Databricks objects
    :param clusters: dictionary generated by ``identify_clusters`` function
    :param jobs: dictionary generated by ``identify_jobs`` function
    :param dlts: dictionary generated by ``identify_dlt_pipelines`` function
    :param policies: dictionary generated by ``identify_cluster_policies`` function
    :param clusters_with_named: dictionary generated by ``identify_clusters_with_named_scripts`` function
    :return: list of uniq init scripts file names
    """
    init_scripts = set()
    for v in (clusters or {}).values():
        init_scripts.update(v["scripts"])
    for v in (clusters_with_named or {}).values():
        init_scripts.update(v["scripts"])    
    for v in (jobs or {}).values():
        init_scripts.update(v["scripts"].keys())
    for v in (dlts or {}).values():
        init_scripts.update(v["scripts"].keys())
    for v in (policies or {}).values():
        init_scripts.update(v["scripts"])

    return list(init_scripts)


def check_dbfs_file_references(wc: WorkspaceClient, init_scripts: List[str]) -> List[str]:
    """
    Checks a list of init scripts on DBFS for references to other files on DBFS (searching for '/dbfs' string)
    :param wc: Databricks workspace client
    :param init_scripts: list of init scripts to analyze
    :return: list of init scripts that have references to files on DBFS
    """
    dbfs_refs = []
    log.info(f"Got {len(init_scripts)} scrips to analyze. Checking for DBFS references...")
    for init_script in init_scripts:
        try:
            f = wc.dbfs.read(init_script)
            if f.data:
                content = base64.b64decode(f.data)
                if content.find(b'/dbfs') != -1:
                    dbfs_refs.append(init_script)
        except DatabricksError as ex:
            log.info(f"Error reading DBFS file '{init_script}': {ex}")

    return dbfs_refs
