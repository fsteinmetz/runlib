#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dask_jobqueue import HTCondorCluster
from dask.distributed import Client


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
        self.cluster = HTCondorCluster(cores=n_cpus, memory=f"{memory}MB", disk=disk)
        self.cluster.adapt(maximum_jobs=maximum_jobs)
        self.client = Client(self.cluster)
        print('Dask daskboard link:', self.client.dashboard_link)

    def map(self, function, *iterables):
        futures = self.client.map(function, *iterables)
        return self.client.gather(futures)
