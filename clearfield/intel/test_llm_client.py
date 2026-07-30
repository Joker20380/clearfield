import json
import urllib.error
from unittest.mock import Mock, patch

from django.test import SimpleTestCase, override_settings

from intel.llm.ollama_client import generate_with_ollama


class _Response:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps(
            {
                "model": "test-model",
                "choices": [{"message": {"content": '{"ok": true}'}}],
            }
        ).encode()


@override_settings(
    LLM_ENABLED=True,
    OLLAMA_BASE_URL="https://llm.example",
    OLLAMA_MODEL="test-model",
)
class OllamaClientTests(SimpleTestCase):
    @patch("intel.llm.ollama_client.time.sleep")
    @patch("intel.llm.ollama_client.urllib.request.urlopen")
    def test_retries_transient_connection_error(
        self,
        urlopen: Mock,
        sleep: Mock,
    ):
        urlopen.side_effect = [
            urllib.error.URLError("temporary"),
            _Response(),
        ]

        result = generate_with_ollama(
            "test",
            timeout_seconds=17,
            max_tokens=321,
            retries=1,
        )

        self.assertEqual(result.text, '{"ok": true}')
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 17)
        sleep.assert_called_once_with(1)

        request = urlopen.call_args.args[0]
        payload = json.loads(request.data.decode())
        self.assertEqual(payload["max_tokens"], 321)
