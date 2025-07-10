"""Read and provide runtime configuration."""

import logging
import os

from pydantic import BaseModel

from therapy.schemas import ServiceEnvironment

_logger = logging.getLogger(__name__)


_ENV_VARNAME = "THERAPY_NORMALIZER_ENV"


class Config(BaseModel):
    """Define app configuration data object."""

    env: ServiceEnvironment
    debug: bool
    test: bool


def _dev_config() -> Config:
    """Provide development environment configs

    :return: dev env configs
    """
    return Config(env=ServiceEnvironment.DEV, debug=True, test=False)


def _test_config() -> Config:
    """Provide test env configs

    :return: test configs
    """
    return Config(env=ServiceEnvironment.TEST, debug=False, test=True)


def _staging_config() -> Config:
    """Provide staging env configs

    :return: staging configs
    """
    return Config(env=ServiceEnvironment.STAGING, debug=False, test=False)


def _prod_config() -> Config:
    """Provide production configs

    :return: prod configs
    """
    return Config(env=ServiceEnvironment.PROD, debug=False, test=False)


def _default_config() -> Config:
    """Provide default configs. This function sets what they are.

    :return: default configs
    """
    return _dev_config()


_CONFIG_MAP = {
    ServiceEnvironment.DEV: _dev_config,
    ServiceEnvironment.TEST: _test_config,
    ServiceEnvironment.STAGING: _staging_config,
    ServiceEnvironment.PROD: _prod_config,
}


def _set_config() -> Config:
    """Set configs based on environment variable `THERAPY_NORMALIZER_ENV`.

    :return: complete config object with environment-specific parameters
    """
    raw_env_value = os.environ.get(_ENV_VARNAME)
    if not raw_env_value:
        return _default_config()
    try:
        env_value = ServiceEnvironment(raw_env_value.lower())
    except ValueError:
        _logger.warning(
            "Unrecognized value for %s: '%s'. Using default configs",
            _ENV_VARNAME,
            raw_env_value,
        )
        return _default_config()
    return _CONFIG_MAP[env_value]()


config = _set_config()


__all__ = ["config"]
