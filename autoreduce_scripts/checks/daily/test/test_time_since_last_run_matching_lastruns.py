from autoreduce_scripts.checks import setup_django
from pathlib import Path
from unittest.mock import patch
import shutil

from autoreduce_db.reduction_viewer.models import Instrument, ReductionRun
from autoreduce_qp.queue_processor.settings import ARCHIVE_ROOT
from django.contrib.staticfiles.testing import LiveServerTestCase
from django.utils import timezone

from autoreduce_scripts.checks.daily.time_since_last_run import main

# pylint:disable=no-member,no-self-use

setup_django()


class TimeSinceLastRunMatchingLastrunsTest(LiveServerTestCase):
    """
    Test the behaviour when the last runs matches the lastruns.txt contents
    """
    fixtures = ["status_fixture", "multiple_instruments_and_runs"]

    def setUp(self) -> None:
        self.instruments = Instrument.objects.all()
        for instrument in self.instruments:
            log_path = Path(ARCHIVE_ROOT, f"NDX{instrument}", "logs")
            log_path.mkdir(parents=True, exist_ok=True)
            last_runs_txt = log_path / "lastrun.txt"
            last_runs_txt.write_text(f"{instrument} {instrument.reduction_runs.last().run_number} 0")

    def tearDown(self) -> None:
        for instrument in self.instruments:
            log_path = Path(ARCHIVE_ROOT, f"NDX{instrument}", "logs")
            shutil.rmtree(log_path)

    @patch("autoreduce_scripts.checks.daily.time_since_last_run.logging")
    def test_with_multiple_instruments(self, mock_logging):
        """
        Test when there are multiple instruments that haven't had run in a day, but they are also
        the last runs recorded in lastruns.txt - we are not expecting anything to be logged, as the
        beamline hasn't had any new runs that need processing
        """
        main()
        mock_logging.getLogger.return_value.warning.assert_not_called()

    # @patch("autoreduce_scripts.checks.daily.time_since_last_run.logging")
    # def test_only_one_doesnt_have_runs(self, mock_logging):
    #     """
    #     Test when one instrument hasn't had runs, but one has.
    #     Only one of them should cause a log message.
    #     """
    #     rr2 = ReductionRun.objects.get(pk=2)
    #     rr2.created = timezone.now()
    #     rr2.save()
    #     main()
    #     mock_logging.getLogger.return_value.warning.assert_called_once()

    # @patch("autoreduce_scripts.checks.daily.time_since_last_run.logging")
    # def test_all_have_runs(self, mock_logging):
    #     """
    #     Test when one instrument hasn't had runs, but one has.
    #     Only one of them should cause a log message.
    #     """
    #     for redrun in ReductionRun.objects.all():
    #         redrun.created = timezone.now()
    #         redrun.save()
    #     main()
    #     mock_logging.getLogger.return_value.warning.assert_not_called()

    # @patch("autoreduce_scripts.checks.daily.time_since_last_run.logging")
    # def test_paused_instruments_not_reported(self, mock_logging):
    #     """
    #     Test when one instrument hasn't had runs, but one has.
    #     Only one of them should cause a log message.
    #     """
    #     last_instr = Instrument.objects.last()
    #     last_instr.is_active = False
    #     last_instr.save()
    #     main()
    #     mock_logging.getLogger.return_value.warning.assert_called_once()

    # @patch("autoreduce_scripts.checks.daily.time_since_last_run.logging")
    # def test_instrument_without_runs(self, mock_logging):
    #     """
    #     Test when one instrument hasn't had runs, but one has.
    #     Only one of them should cause a log message.
    #     """
    #     last_instr = Instrument.objects.last()
    #     last_instr.reduction_runs.all().delete()
    #     main()
    #     mock_logging.getLogger.return_value.warning.assert_called_once()
