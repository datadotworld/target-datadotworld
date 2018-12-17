# Contributing Guidelines

### Issues
Issue reports are a great way to contribute to this project.  
To the extent possible, make sure that your issue is detailed and not a duplicate.

# Contribute Code

### Fork the Project

Fork the project [on Github](https://github.com/datadotworld/target-datadotworld.git) and check out your copy.

```sh
$ git clone https://github.com/[YOUR_GITHUB_NAME]/target-datadotworld.git
$ cd tap-redshift
$ git remote add upstream https://github.com/datadotworld/target-datadotworld.git
```

### Install and Test

Run the command below to install packages required:

```sh
$ pip install -e .
```

Run tests:

```sh
$ python setup.py test
```

### Setting up a config file

To test and see how the target-datadotworld works, you'll need to provide the necessary configuration.

From the project root, create a `config.json` file. Content should look like;

```json
{
    "api_token": "your_token",
    "dataset_id": "your_dataset_id"
}
```

- `api_token` by enabling the Singer integration at https://data.world/integrations/singer
- `dataset_id` can be obtained from a dataset URL (e.g. `my_dataset` in https://data.world/my_user/my_dataset`)

### Write Tests

Try to write a test that reproduces the problem you're trying to fix or describes a feature that you want to build. Add tests to spec.

We definitely appreciate pull requests that highlight or reproduce a problem, even without a fix.

### Write Code

Implement your feature or bug fix. Make sure that all tests pass without errors.

Also, to make sure that your code follows our coding style guide and best practices, run the command:

```sh
$ flake8
```

### Commit Changes

Make sure git knows your name and email address:

```sh
git config --global user.name "Your Name"
git config --global user.email "contributor@example.com"
```

Writing good commit logs is important. A commit log should describe what changed and why.

```sh
git add ...
git commit
```

### Push

```sh
git push origin my-feature-branch
```

### Make a Pull Request
Go to https://github.com/[YOUR_GITHUB_NAME]/target-datadotworld.git and select your feature branch. Click the 'Pull Request' button and fill out the form. Pull requests are usually reviewed within a few days.

# Release (for maintainers)

Checklist:

- Build passes `tox` verification (all tests across versions, test coverage, and code style)
- Version number is correct in `target_datadotworld/__init__.py`
- All docs are updated, including `README.rst` and `/docs`

Release process:

1. Create a GitHub release (and respective tag)
2. Push respective tag to `release` branch (i.e. `git push origin [tag]^{}:release`)

**NOTE:** `tap-redshift` is released to PyPI and DockerHub at the same time.

# Thank you!
Thank you in advance, for contributing to this project!
