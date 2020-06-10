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
from substra.sdk import schemas
from substra.sdk.backends import base
from substra.sdk.backends.local import models
from substra.sdk.backends.local import db
from substra.sdk.backends.local import fs
from substra.sdk.backends.local import hasher
from substra.sdk.backends.local import compute

_BACKEND_ID = 'local-backend'


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

    def _add_dataset(self, spec, exist_ok, spec_options):
        asset = models.Dataset(
            key=fs.hash_file(spec.data_opener),
            owner=_BACKEND_ID,
            name=spec.name,
            objective_key=spec.objective_key if spec.objective_key else '',
            permissions={
                'process': {
                    'public': spec.permissions.public,
                    'authorized_ids': spec.permissions.authorized_ids,
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
        datasets = [
            self._db.get(schemas.Type.Dataset, dataset_key)
            for dataset_key in spec.data_manager_keys
        ]

        data_sample = models.DataSample(
            key=fs.hash_directory(spec.path),
            owner=_BACKEND_ID,
            path=spec.path,
            data_manager_keys=spec.data_manager_keys,
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
                owner=_BACKEND_ID,
                path=p,
                data_manager_keys=spec.data_manager_keys,
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
        # validate spec
        if spec.test_data_manager_key:
            dataset = self._db.get(
                schemas.Type.Dataset, spec.test_data_manager_key)

        # validate test data samples
        for key in spec.test_data_sample_keys:
            self._db.get(schemas.Type.DataSample, key)

        # create objective model instance
        objective = models.Objective(
            key=fs.hash_file(spec.metrics),
            name=spec.name,
            owner=_BACKEND_ID,
            test_dataset={
                'dataset_key': spec.test_data_manager_key,
                'data_sample_keys': spec.test_data_sample_keys,
            },
            permissions={
                'process': {
                    'public': spec.permissions.public,
                    'authorized_ids': spec.permissions.authorized_ids,
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
        algo = model_class(
            key=fs.hash_file(spec.file),
            name=spec.name,
            owner=_BACKEND_ID,
            permissions={
                'process': {
                    'public': spec.permissions.public,
                    'authorized_ids': spec.permissions.authorized_ids,
                },
            },
            file=spec.file,
            description=spec.description,
        )
        return self._db.add(algo, exist_ok)

    def _add_algo(self, spec, exist_ok, spec_options=None):
        return self.__add_algo(models.Algo, spec, exist_ok, spec_options=spec_options)

    def _add_aggregate_algo(self, spec, exist_ok, spec_options=None):
        return self.__add_algo(models.AggregateAlgo, spec, exist_ok, spec_options=spec_options)

    def _add_composite_algo(self, spec, exist_ok, spec_options=None):
        return self.__add_algo(models.CompositeAlgo, spec, exist_ok, spec_options=spec_options)

    def _add_traintuple(self, spec, exist_ok, spec_options=None):
        # validation
        self._db.get(schemas.Type.Algo, spec.algo_key)
        self._db.get(schemas.Type.Dataset, spec.data_manager_key)
        in_traintuples = [
            self._db.get(schemas.Type.Traintuple, key)
            for key in spec.in_models_keys
        ]

        key_components = [_BACKEND_ID, spec.algo_key, spec.data_manager_key] \
            + spec.train_data_sample_keys \
            + spec.in_models_keys
        key = hasher.Hasher(values=key_components).compute()

        # create model
        options = {}
        traintuple = models.Traintuple(
            key=key,
            creator=_BACKEND_ID,
            worker=_BACKEND_ID,
            algo_key=spec.algo_key,
            dataset={
                'opener_hash': spec.data_manager_key,
                'keys': spec.train_data_sample_keys,
                'worker': _BACKEND_ID,
            },
            permissions={
                # TODO implement merge of permissions
                'process': {
                    'public': True,
                    'authorized_ids': [],
                },
            },
            log='',
            compute_plan_id=spec.compute_plan_id or '',
            rank=spec.rank or 0,  # TODO compute a default rank
            tag=spec.tag or '',
            status=models.Status.waiting.value,
            in_models=[{
                'hash': None,
                'storage_address': None,
            } for in_traintuple in in_traintuples],
            **options,
        )

        traintuple = self._db.add(traintuple, exist_ok)
        self._worker.schedule(traintuple)
        return traintuple

    def add(self, spec, exist_ok, spec_options=None):
        # find dynamically the method to call to create the asset
        method_name = f'_add_{spec.__class__.type_.value}'
        if spec.is_many():
            method_name += 's'
        add_asset = getattr(self, method_name)
        asset = add_asset(spec, exist_ok, spec_options)
        if spec.is_many():
            return [a.to_reponse() for a in asset]
        else:
            return asset.to_response()

    def update_compute_plan(self, compute_plan_id, spec):
        raise NotImplementedError

    def link_dataset_with_objective(self, dataset_key, objective_key):
        raise NotImplementedError

    def link_dataset_with_data_samples(self, dataset_key, data_sample_keys):
        raise NotImplementedError

    def download(self, asset_type, url_field_path, key, destination):
        raise NotImplementedError

    def describe(self, asset_type, key):
        raise NotImplementedError

    def leaderboard(self, objective_key, sort='desc'):
        raise NotImplementedError

    def cancel_compute_plan(self, compute_plan_id):
        raise NotImplementedError
