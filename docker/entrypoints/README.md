# Devbox entrypoint pattern

## Usage

The entrypoint of the `devbox` image is set as `docker/entrypoints/entrypoint.sh`.
This script is minimal and allows the devbox to perform three different functions,
depending on what arguments are provided:

1. If no arguments are provided, the devbox container enters into a bash shell and allows a developer to interactively run commands.

    ```console
    me@local > $ docker-compose run --rm devbox
    [jovyan@devbox] $ echo "I'm in an interactive shell"
    ```

1. If the first argument provided to the devbox is `test`, the devbox will enter into testing mode. This replaces the `test` service that was common with many applications previously.

    ```console
    me@local > $ docker-compose run --rm devbox test --help
    usage: entrypoint.sh test (--ci|--local) [--all] [--lint] [--unit-test] [--no-pyenv] [--run|--skip {step} <extra_args>]
    ...
    ```

    Running `docker-compose run --rm devbox test --all` is a drop-in replacement for the `docker-compose run --rm test` command that was commonly used in the past.

1. If arguments other than `test` are provided, these are executed directly by bash.

    ```console
    me@local > $ docker-compose run --rm devbox echo Hello from the devbox
    Hello from the devbox
    ```

## run_tests.sh

The documentation of the local test suite can be found by running `docker-compose run --rm devbox test --help`:

```console
usage: entrypoint.sh test (--ci|--local) [--all] [--lint] [--unit-test] [--no-pyenv] [--run|--skip {step} <extra_args>]

Valid steps: {pytest bandit black flake8 isort mypy}

Options:
 --ci/--local     : Current environment. When run locally, will attempt to fix errors.
 --all            : Run both the unit test and linting suites
 --lint           : Run the full linting suite.
 --unit-test      : Run the full unit test suite.
 --no-pyenv       : Don't activate the pyenv virtualenv
 --run/--skip     : Specify a specific check to run/skip. --run optionally accepts extra
                    arguments that are passed to the step. This option can be used multiple times.
```

### Commonly used commands

(`dcr` is used as an alias for `docker-compose run --rm` below)

* `dcr devbox test --all`: Runs the full linting and unit test suites locally. Will attempt to fix linting issues, if found.
* `dcr devbox test --lint`: Runs only the linting steps. Will attempt to fix linting issues, if found.
* `dcr devbox test --all --skip isort`: Runs the full test suites (as above), but excludes `isort`.
* `dcr devbox test --all --run pytest -vv`: Runs the full test suite (as above), but adds the `-vv` (verbose) option to `pytest`. Any extra arguments can be passed to the available steps in this way.
* `dcr devbox test --ci ...`: The addition of the `--ci` flag runs the tests in CI mode, which will cause the pipeline to fail on error and is

### Updating or adding steps to the test suite

The following considerations must be taken if additional steps are to be added to the test suite:

#### Adding a new step

* There are currently two categories of steps: "lint" and "test". If an additional step is added, it must be included in the appropriate step array: `LINT_STEPS`, and `TEST_STEPS`, respectively. This ensures the new step will be executed if the user provides the `--all`, `--lint`, or `--unit-test` flags.
* Add a new case to the `run_steps` function that will execute the new step, following the pattern used by the existing steps.

#### Adding a new category of tests

* You may choose to add a new category of steps (_e.g._ integration tests.) If you do this, create a new array to store the category's steps and include the array in `ALL_STEPS`.
* You may want to add a new flag to the CLI to support running the new category (_e.g._ `--integration-test`). Include the new flag in the `VALID_CLI_OPTIONS` array. Add a new case to the `handle_input` function, following the pattern used for the existing category groups.
