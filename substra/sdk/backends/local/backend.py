# Copyright 2018 Owkin, inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import functools
import shutil
import uuid

import substra
from substra.sdk import schemas
from substra.sdk.backends import base
from substra.sdk.backends.local import models
from substra.sdk.backends.local import db
from substra.sdk.backends.local import fs
from substra.sdk.backends.local import hasher
from substra.sdk.backends.local import compute

_BACKEND_ID = "local-backend"


class Local(base.BaseBackend):
    def __init__(self, *args, **kwargs):
        # create a store to abstract the db
        self._db = db.get()
        self._worker = compute.Worker()

    def get(self, asset_type, key):
        return self._db.get(asset_type, key).to_response()

    def list(self, asset_type, filters=None):
        assets = self._db.list(asset_type)
        return [a.to_response() for a in assets]

    def __compute_permissions(self, permissions):
        """Compute the permissions

        If the permissions are private, the active node is
        in the authorized ids.
        """
        if permissions.public:
            permissions.authorized_ids = list()
        elif not permissions.public and _BACKEND_ID not in permissions.authorized_ids:
            permissions.authorized_ids.append(_BACKEND_ID)
        return permissions

    def _add_dataset(self, spec, exist_ok, spec_options):
        permissions = self.__compute_permissions(spec.permissions)
        asset = models.Dataset(
            key=fs.hash_file(spec.data_opener),
            pkhash=fs.hash_file(spec.data_opener),
            owner=_BACKEND_ID,
            name=spec.name,
            objective_key=spec.objective_key if spec.objective_key else "",
            permissions={
                "process": {
                    "public": permissions.public,
                    "authorized_ids": permissions.authorized_ids,
                },
            },
            type=spec.type,
            train_data_sample_keys=[],
            test_data_sample_keys=[],
            data_opener=spec.data_opener,
            description=spec.description,
        )
        return self._db.add(asset, exist_ok)

    def _add_data_sample(self, spec, exist_ok, spec_options):
        assert len(spec.data_manager_keys) > 0
        datasets = [
            self._db.get(schemas.Type.Dataset, dataset_key)
            for dataset_key in spec.data_manager_keys
        ]

        data_sample = models.DataSample(
            key=fs.hash_directory(spec.path),
            pkhash=fs.hash_directory(spec.path),
            owner=_BACKEND_ID,
            path=spec.path,
            data_manager_keys=spec.data_manager_keys,
            test_only=spec.test_only,
        )
        data_sample = self._db.add(data_sample, exist_ok)

        # update dataset(s) accordingly
        for dataset in datasets:
            if spec.test_only:
                samples_list = dataset.test_data_sample_keys
            else:
                samples_list = dataset.train_data_sample_keys
            if data_sample.key not in samples_list:
                samples_list.append(data_sample.key)

        return data_sample

    def _add_data_samples(self, spec, exist_ok, spec_options):
        datasets = [
            self._db.get(schemas.Type.Dataset, dataset_key)
            for dataset_key in spec.data_manager_keys
        ]

        data_samples = [
            models.DataSample(
                key=fs.hash_directory(p),
                pkhash=fs.hash_directory(p),
                owner=_BACKEND_ID,
                path=p,
                data_manager_keys=spec.data_manager_keys,
                test_only=spec.test_only,
            )
            for p in spec.paths
        ]

        data_samples = [self._db.add(a) for a in data_samples]

        # update dataset(s) accordingly
        for dataset in datasets:
            if spec.test_only:
                samples_list = dataset.test_data_sample_keys
            else:
                samples_list = dataset.train_data_sample_keys

            for data_sample in data_samples:
                if data_sample.key not in samples_list:
                    samples_list.append(data_sample.key)

        return data_samples

    def _add_objective(self, spec, exist_ok, spec_options):
        permissions = self.__compute_permissions(spec.permissions)

        # validate spec
        test_dataset = None
        if spec.test_data_manager_key:
            dataset = self._db.get(schemas.Type.Dataset, spec.test_data_manager_key)
            # validate test data samples
            for key in spec.test_data_sample_keys:
                self._db.get(schemas.Type.DataSample, key)
            test_dataset = {
                "dataset_key": spec.test_data_manager_key,
                "data_sample_keys": spec.test_data_sample_keys,
            }

        # create objective model instance
        objective = models.Objective(
            key=fs.hash_file(spec.metrics),
            pkhash=fs.hash_file(spec.metrics),
            name=spec.name,
            owner=_BACKEND_ID,
            test_dataset=test_dataset,
            permissions={
                "process": {
                    "public": permissions.public,
                    "authorized_ids": permissions.authorized_ids,
                },
            },
            description=spec.description,
            metrics=spec.metrics,
        )

        # add objective to storage and update optionnally the associated dataset
        objective = self._db.add(objective, exist_ok)
        if spec.test_data_manager_key:
            dataset.objective_key = objective.key

        return objective

    def __add_algo(self, model_class, spec, exist_ok, spec_options=None):
        permissions = self.__compute_permissions(spec.permissions)
        algo = model_class(
            key=fs.hash_file(spec.file),
            pkhash=fs.hash_file(spec.file),
            name=spec.name,
            owner=_BACKEND_ID,
            permissions={
                "process": {
                    "public": permissions.public,
                    "authorized_ids": permissions.authorized_ids,
                },
            },
            file=spec.file,
            description=spec.description,
        )
        return self._db.add(algo, exist_ok)

    def _add_algo(self, spec, exist_ok, spec_options=None):
        return self.__add_algo(models.Algo, spec, exist_ok, spec_options=spec_options)

    def _add_aggregate_algo(self, spec, exist_ok, spec_options=None):
        return self.__add_algo(
            models.AggregateAlgo, spec, exist_ok, spec_options=spec_options
        )

    def _add_composite_algo(self, spec, exist_ok, spec_options=None):
        return self.__add_algo(
            models.CompositeAlgo, spec, exist_ok, spec_options=spec_options
        )

    def __intersect_permissions(self, aggregate_set, element):
        if not element.permissions.process.public:
            if aggregate_set is None:
                aggregate_set = set(element.permissions.process.authorized_ids)
            else:
                aggregate_set = aggregate_set & set(
                    element.permissions.process.authorized_ids
                )
        return aggregate_set

    def __add_compute_plan(
        self,
        traintuple_keys=None,
        composite_traintuple_keys=None,
        aggregatetuple_keys=None,
        testtuple_keys=None,
    ):
        tuple_count = functools.reduce(
            lambda x, y: x + (len(y) if y else 0),
            [
                traintuple_keys,
                composite_traintuple_keys,
                aggregatetuple_keys,
                testtuple_keys,
            ],
            0,
        )
        compute_plan = models.ComputePlan(
            compute_plan_id=uuid.uuid4().hex,
            status=models.Status.waiting.value,
            traintuple_keys=traintuple_keys,
            composite_traintuple_keys=composite_traintuple_keys,
            aggregatetuple_keys=aggregatetuple_keys,
            testtuple_keys=testtuple_keys,
            id_to_key=dict(),
            tag="",
            tuple_count=tuple_count,
            done_count=0,
        )
        return self._db.add(compute_plan)

    def _add_traintuple(self, spec, exist_ok, spec_options=None):
        # validation
        algo = self._db.get(schemas.Type.Algo, spec.algo_key)
        data_manager = self._db.get(schemas.Type.Dataset, spec.data_manager_key)
        in_traintuples = (
            [self._db.get(schemas.Type.Traintuple, key) for key in spec.in_models_keys]
            if spec.in_models_keys is not None
            else []
        )

        key_components = (
            [_BACKEND_ID, spec.algo_key, spec.data_manager_key]
            + spec.train_data_sample_keys
            + spec.in_models_keys
            if spec.in_models_keys is not None
            else []
        )
        key = hasher.Hasher(values=key_components).compute()

        # permissions
        with_permissions = [algo, data_manager] + in_traintuples

        authorized_ids = functools.reduce(
            self.__intersect_permissions, with_permissions, None
        )
        public = authorized_ids is None
        authorized_ids = list() if authorized_ids is None else list(authorized_ids)

        # compute plan and rank
        if not spec.compute_plan_id and spec.rank == 0:
            #  Create a compute plan
            compute_plan = self.__add_compute_plan(traintuple_keys=[key])
            rank = 0
            compute_plan_id = compute_plan.compute_plan_id
        elif not spec.compute_plan_id and spec.rank is not None:
            raise ValueError("Rank must be 0 to create compute plan.")
        elif spec.compute_plan_id:
            compute_plan = self._db.get(schemas.Type.ComputePlan, spec.compute_plan_id)
            compute_plan_id = compute_plan.compute_plan_id
            rank = (
                len(compute_plan.traintuple_keys)
                if compute_plan.traintuple_keys is not None
                else 0 + len(compute_plan.composite_traintuple_keys)
                if compute_plan.composite_traintuple_keys is not None
                else 0 + len(compute_plan.aggregatetuple_keys)
                if compute_plan.aggregatetuple_keys is not None
                else 0
            )
            #  Add to the compute plan
            if compute_plan.traintuple_keys is None:
                compute_plan.traintuple_keys = [key]
            else:
                compute_plan.traintuple_keys.append(key)
            compute_plan.tuple_count += 1
            compute_plan.status = models.Status.waiting

        else:
            compute_plan_id = ""
            rank = 0

        # create model
        options = {}
        traintuple = models.Traintuple(
            key=key,
            creator=_BACKEND_ID,
            worker=_BACKEND_ID,
            algo_key=spec.algo_key,
            dataset={
                "opener_hash": spec.data_manager_key,
                "keys": spec.train_data_sample_keys,
                "worker": _BACKEND_ID,
            },
            permissions={
                "process": {"public": public, "authorized_ids": authorized_ids},
            },
            log="",
            compute_plan_id=compute_plan_id,
            rank=rank,
            tag=spec.tag or "",
            status=models.Status.waiting.value,
            in_models=[
                {
                    "hash": in_traintuple.out_model.hash_,
                    "storage_address": in_traintuple.out_model.storage_address,
                }
                for in_traintuple in in_traintuples
            ],
            **options,
        )

        traintuple = self._db.add(traintuple, exist_ok)
        self._worker.schedule_traintuple(traintuple)
        return traintuple

    def _add_testtuple(self, spec, exist_ok, spec_options=None):

        # validation
        objective = self._db.get(schemas.Type.Objective, spec.objective_key)
        traintuple = self._db.get(schemas.Type.Traintuple, spec.traintuple_key)
        assert traintuple.out_model is not None
        if spec.data_manager_key is not None:
            self._db.get(schemas.Type.Dataset, spec.data_manager_key)
        if spec.test_data_sample_keys is not None:
            [
                self._db.get(schemas.Type.DataSample, key)
                for key in spec.test_data_sample_keys
            ]

        # Hash creation
        key_components = [_BACKEND_ID, spec.objective_key, spec.traintuple_key] + (
            spec.test_data_sample_keys if spec.test_data_sample_keys is not None else []
        )
        if spec.data_manager_key is not None:
            key_components += spec.data_manager_key
        key = hasher.Hasher(values=key_components).compute()

        # create model
        # if dataset is not defined, take it from objective
        if spec.data_manager_key:
            assert (
                spec.test_data_sample_keys is not None
                and len(spec.test_data_sample_keys) > 0
            )
            dataset_opener = spec.data_manager_key
            test_data_sample_keys = spec.test_data_sample_keys
            certified = (
                objective.test_dataset is not None
                and objective.test_dataset.dataset_key == spec.data_manager_key
                and set(objective.test_dataset.data_sample_keys)
                == set(spec.test_data_sample_keys)
            )
        else:
            assert (
                objective.test_dataset
            ), "can not create a certified testtuple, no data associated with objective"
            dataset_opener = objective.test_dataset.dataset_key
            test_data_sample_keys = objective.test_dataset.data_sample_keys
            certified = True

        if traintuple.compute_plan_id:
            compute_plan = self._db.get(
                schemas.Type.ComputePlan, traintuple.compute_plan_id
            )
            if compute_plan.testtuple_keys is None:
                compute_plan.testtuple_keys = [key]
            else:
                compute_plan.testtuple_keys.append(key)
            compute_plan.tuple_count += 1
            compute_plan.status = models.Status.waiting

        options = {}
        testtuple = models.Testtuple(
            key=key,
            creator=_BACKEND_ID,
            objective_key=spec.objective_key,
            traintuple_key=spec.traintuple_key,
            certified=certified,
            dataset={
                "opener_hash": dataset_opener,
                "perf": -1,
                "keys": test_data_sample_keys,
                "worker": _BACKEND_ID,
            },
            log="",
            tag=spec.tag or "",
            status=models.Status.waiting.value,
            rank=traintuple.rank,
            compute_plan_id=traintuple.compute_plan_id,
            **options,
        )
        testtuple = self._db.add(testtuple, exist_ok)
        self._worker.schedule_testtuple(testtuple)
        return testtuple

    def _download_algo(self, url_field_path, key, destination):
        asset = self._db.get(type_=schemas.Type.Algo, key=key)
        shutil.copyfile(asset.file, destination)

    def _download_dataset(self, url_field_path, key, destination):
        asset = self._db.get(type_=schemas.Type.Dataset, key=key)
        shutil.copyfile(asset.data_opener, destination)

    def _download_objective(self, url_field_path, key, destination):
        asset = self._db.get(type_=schemas.Type.Objective, key=key)
        shutil.copyfile(asset.metrics, destination)

    def add(self, spec, exist_ok, spec_options=None):
        # find dynamically the method to call to create the asset
        method_name = f"_add_{spec.__class__.type_.value}"
        if spec.is_many():
            method_name += "s"
        add_asset = getattr(self, method_name)
        asset = add_asset(spec, exist_ok, spec_options)
        if spec.is_many():
            return [a.to_response() for a in asset]
        else:
            return asset.to_response()

    def update_compute_plan(self, compute_plan_id, spec):
        raise NotImplementedError

    def link_dataset_with_objective(self, dataset_key, objective_key):
        # validation
        dataset = self._db.get(schemas.Type.Dataset, dataset_key)
        self._db.get(schemas.Type.Objective, objective_key)
        if dataset.objective_key:
            raise substra.exceptions.InvalidRequest(
                "Dataset already linked to an objective", 400
            )

        dataset.objective_key = objective_key
        return {"pkhash": dataset.key}

    def link_dataset_with_data_samples(self, dataset_key, data_sample_keys):
        dataset = self._db.get(schemas.Type.Dataset, dataset_key)
        data_samples = list()
        for key in data_sample_keys:
            data_sample = self._db.get(schemas.Type.DataSample, key)
            if dataset_key not in data_sample.data_manager_keys:
                data_sample.data_manager_keys.append(dataset_key)
                if data_sample.test_only:
                    dataset.test_data_sample_keys.append(key)
                else:
                    dataset.train_data_sample_keys.append(key)
            else:
                print(f"Data sample already in dataset: {key}")
            data_samples.append(data_sample)

    def download(self, asset_type, url_field_path, key, destination):
        method_name = f"_download_{asset_type.value}"
        download_asset = getattr(self, method_name)
        download_asset(url_field_path, key, destination)

    def describe(self, asset_type, key):
        asset = self._db.get(type_=asset_type, key=key)
        if not hasattr(asset, "description") or not asset.description:
            raise ValueError("This element does not have a description.")
        with open(asset.description, "r", encoding="utf-8") as f:
            return f.read()

    def leaderboard(self, objective_key, sort="desc"):
        raise NotImplementedError

    def cancel_compute_plan(self, compute_plan_id):
        raise NotImplementedError
