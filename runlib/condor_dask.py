#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
from pathlib import Path
import tempfile
from dask_jobqueue import HTCondorCluster
from dask.distributed import Client, as_completed
import getpass


class CondorPool:

    def __init__(
        self,
        memory=2000,
        n_cpus=1,
        maximum_jobs=99,
        disk="1GB",
    ) -> None:
        """
        CondorPool-like wrapper around dask distributed for running jobs with dask
        on HTCondor
        """
        logdir = (
            Path(tempfile.gettempdir())
            / f"condor-dask-{getpass.getuser()}"
            / datetime.now().isoformat()
        )
        self.cluster = HTCondorCluster(cores=n_cpus, memory=f"{memory}MB", disk=disk,
                                       log_directory=logdir)
        self.cluster.adapt(maximum_jobs=maximum_jobs)
        self.client = Client(self.cluster)
        print('Dask daskboard link:', self.client.dashboard_link)

    def map(self, function, *iterables):
        futures = self.client.map(function, *iterables)
        return self.client.gather(futures)

    def imap_unordered(self, function, *iterables):
        futures = self.client.map(function, *iterables)
        for future in as_completed(futures):
            yield future.result()
