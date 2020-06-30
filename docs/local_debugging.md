# Local installation of Substra for debugging (macOS & Ubuntu/Debian)

- [Local install of Substra for debugging (macOS & Ubuntu/Debian)](#local-installation-of-substra-for-debugging)
  - [Dependencies](#dependencies)
  - [Configuration](#configuration)
    - [Docker](#docker)
  - [Get source code](#get-source-code)
  - [Run your code locally](#run-your-code-locally)

## Dependencies

- Docker:
  - Mac: [Docker Desktop](https://www.docker.com/products/docker-desktop)
  - Ubuntu/Debian: `sudo apt install docker docker-compose`
- Python 3 (recommended 3.6 or 3.7)
  - It is recommended to use a virtual environment to install Substra packages (for instance [virtualenv](https://virtualenv.pypa.io/en/latest/))

## Configuration

### Docker

- On macOS, by default, docker has access only to the user directory.
  - Substra requires access to a local folder that you can set through the `SUBSTRA_PATH` variable env (defaults to `/tmp/substra`). Make sure the directory of your choice is accessible by updating accordingly the docker desktop configuration (`Preferences` > `File Sharing`).
    - Also ensure that the docker daemon has enough resources to execute the ML pipeline, for instance: CPUs>1, Memory>4.0 GiB (`Preferences` > `Advanced`).
- On Linux environment, please refer to this [guide](https://github.com/SubstraFoundation/substra-backend/blob/master/doc/linux-userns-guide.md) to configure docker.

## Get source code

- Define a root directory for all your Substra git repositories, for instance `~/substra`:

```sh
export SUBSTRA_SOURCE=~/substra
mkdir -p $SUBSTRA_SOURCE
cd $SUBSTRA_SOURCE
```

- Clone the following repositories from [Substra's Github](https://github.com/SubstraFoundation):
  - [substra](https://github.com/SubstraFoundation/substra.git)

```sh
git clone https://github.com/SubstraFoundation/substra.git
```

> Note: if you do not have `git` on your machine, you can also download and unzip in the same folder the code using this link:
>
> - [substra](https://github.com/SubstraFoundation/substra/archive/master.zip)

## Run your code locally

With this installation, you can write scripts using the [SDK](../references/sdk.md) as you would for a remote exceution, and set the client backend as 'local' to run the script locally.

When it runs locally, there are a few constraints:
- the setup is "one user, one node", so no communication between nodes
- the data is saved in memory, so all the code should be in one script

The execution is done synchronously, so the script waits for the train / predict to end before continuing.
The execution of the tuples happens in Docker containers that are spawned on the fly and removed once the execution is done.
If you want access to the container while it runs, use the [`input`](https://docs.python.org/3.6/library/functions.html#input) function or any function that needs a user input to terminate to pause the execution until you connect to the container.