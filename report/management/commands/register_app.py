from django.core.management.base import BaseCommand
from report.utils import register_app


class Command(BaseCommand):
    help = 'backupd db'

    def add_arguments(self, parser):
        parser.add_argument('app_name', nargs='?', type=str)

    def handle(self, *args, **options):
        register_app(options['app_name'])
