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
import os
import shutil

from substra.sdk import schemas
from substra.sdk.backends.local import db
from substra.sdk.backends.local import models
from substra.sdk.backends.local import fs
from substra.sdk.backends.local.compute import spawner

_CONTAINER_MODEL_PATH = '/sandbox/model'

_VOLUME_INPUT_DATASAMPLES = {'bind': '/sandbox/data', 'mode': 'ro'}
_VOLUME_MODELS_RO = {'bind': _CONTAINER_MODEL_PATH, 'mode': 'ro'}
_VOLUME_MODELS_RW = {'bind': _CONTAINER_MODEL_PATH, 'mode': 'rw'}
_VOLUME_OPENER = {'bind': '/sandbox/opener/__init__.py', 'mode': 'ro'}
_VOLUME_OUTPUT_PRED = {'bind': '/sandbox/pred', 'mode': 'rw'}
_VOLUME_LOCAL = {'bind': '/sandbox/local', 'mode': 'rw'}


def _mkdir(path, delete_if_exists=False):
    """Make directory (recursive)."""
    if os.path.exists(path):
        if not delete_if_exists:
            return path
        shutil.rmtree(path)
    os.makedirs(path)
    return path


class Worker:
    """ML Worker."""
    def __init__(self):
        self._wdir = os.path.join(os.getcwd(), 'local-worker')
        self._db = db.get()
        self._spawner = spawner.get()

    def schedule(self, tuple_):
        """Schedules a ML task (blocking)."""
        # TODO handle all tuple types
        # TODO create a schedule context to clean everything
        tuple_.status = models.Status.doing

        # fetch dependencies
        algo = self._db.get(schemas.Type.Algo, tuple_.algo_key)
        dataset = self._db.get(schemas.Type.Dataset, tuple_.dataset.key)

        # prepare input models and datasamples
        tuple_dir = _mkdir(os.path.join(self._wdir, tuple_.key))
        models_volume = _mkdir(os.path.join(tuple_dir, 'models'))
        for model in tuple_.in_models:
            os.link(model.storage_address, os.path.join(models_volume, model.key))

        data_volume = _mkdir(os.path.join(tuple_dir, 'data'))
        samples = [self._db.get(schemas.Type.DataSample, key)
                   for key in tuple_.dataset.keys]
        for sample in samples:
            # TODO more efficient link (symlink?)
            shutil.copytree(sample.path, os.path.join(data_volume, sample.key))

        volumes = {
            dataset.data_opener: _VOLUME_OPENER,
            data_volume: _VOLUME_INPUT_DATASAMPLES,
            models_volume: _VOLUME_MODELS_RW,
        }

        if tuple_.compute_plan_id:
            local_volume = _mkdir(
                os.path.join(self._wdir, 'compute_plans', 'local', tuple_.compute_plan_id))
            volumes[local_volume] = _VOLUME_LOCAL

        # compute traintuple command
        command = f'train --rank {tuple_.rank}'
        for model in tuple_.in_models:
            command += f' {model.key}'

        container_name = f'algo-{algo.key}'
        logs = self._spawner.spawn(container_name, str(algo.file), command,
                                   volumes=volumes)

        # save move output models
        tmp_path = os.path.join(models_volume, 'model')
        model_dir = _mkdir(os.path.join(self._wdir, 'models', tuple_.key))
        model_path = os.path.join(model_dir, 'model')
        shutil.copy(tmp_path, model_path)

        # delete tuple working directory
        shutil.rmtree(tuple_dir)

        # set logs and status
        tuple_.log = "\n".join(logs)
        tuple_.status = models.Status.done
        tuple_.out_model = models.OutModel(
            hash=fs.hash_file(model_path),
            storage_address=model_path,
        )
