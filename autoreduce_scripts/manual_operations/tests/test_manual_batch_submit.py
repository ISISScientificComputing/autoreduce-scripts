from autoreduce_scripts.manual_operations.tests.test_manual_remove import create_experiment_and_instrument
from unittest.mock import Mock, call, patch

from django.test import TestCase

from autoreduce_scripts.manual_operations.manual_batch_submit import main as submit_batch_main


# pylint:disable=no-self-use
class TestManualBatchSubmission(TestCase):
    """
    Test manual_submission.py
    """
    fixtures = ["status_fixture"]

    def setUp(self) -> None:
        self.experiment, self.instrument = create_experiment_and_instrument()

    @patch('autoreduce_scripts.manual_operations.manual_batch_submit.login_queue')
    @patch('autoreduce_scripts.manual_operations.manual_batch_submit.submit_run')
    @patch('autoreduce_scripts.manual_operations.manual_batch_submit.get_location_and_rb',
           return_value=("test_location", "test_rb"))
    def test_main(self, mock_get_location_and_rb: Mock, mock_submit_run: Mock, mock_login_queue: Mock):
        """Tests the main function of the manual batch submission"""
        runs = (12345, 12346)
        submit_batch_main(self.instrument.name, *runs)
        mock_login_queue.assert_called_once()

        mock_get_location_and_rb.assert_has_calls(
            [call(self.instrument.name, runs[0], "nxs"),
             call(self.instrument.name, runs[1], "nxs")])

        mock_submit_run.assert_called_once_with(mock_login_queue.return_value, ["test_rb", "test_rb"],
                                                self.instrument.name, ["test_location", "test_location"], runs)
