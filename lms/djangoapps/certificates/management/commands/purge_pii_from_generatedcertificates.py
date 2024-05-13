"""
A management command, designed to be run once by Open edX Operators, to obfuscate learner PII from the
`Certificates_GeneratedCertificate` table that should have been purged during learner retirement.

A fix has been included in the retirement pipeline to properly purge this data during learner retirement. This can be
used to purge PII from accounts that have already been retired.
"""

import logging

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from lms.djangoapps.certificates.models import GeneratedCertificate
from openedx.core.djangoapps.user_api.api import get_retired_user_ids

User = get_user_model()
log = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    This management command performs a bulk update on `GeneratedCertificate` instances. This means that it will not
    invoke the custom save() function defined as part of the `GeneratedCertificate` model, and thus will not emit any
    Django signals throughout the system after the update occurs. This is desired behavior. We are using this
    management command to purge leftover PII in the monolith, and we don't need updates to propogate to external systems
    (like the Credentials IDA).

    This management command functions by requesting a list of learners' user_ids whom have completed their journey
    through the retirement pipeline. The `get_retired_user_ids` utility function is responsible for removing any
    learners in the PENDING state, as they could still submit a request to cancel their account deletion request (and
    we don't want to remove any data that may still be good).
    """

    help = """
    Purges learners' full names from the `Certificates_GeneratedCertificate` table if their account has been
    successfully retired.
    """

    def handle(self, * args, **options):
        retired_user_ids = get_retired_user_ids()
        log.warning(
            f"Purging `name` from the certificate records of the following users: {retired_user_ids}"
        )
        GeneratedCertificate.objects.filter(user_id__in=retired_user_ids).update(name="")
