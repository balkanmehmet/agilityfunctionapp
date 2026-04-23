import os
import requests
import logging

class AzureSpeechClient:
    def __init__(self) -> None:
        self.key = os.getenv("AZURE_SPEECH_KEY")
        self.region = os.getenv("AZURE_SPEECH_REGION")
        self.voice = os.getenv("AZURE_SPEECH_VOICE", "en-US-AvaMultilingualNeural")
        self.default_leading_silence_ms = int(os.getenv("AZURE_SPEECH_LEADING_SILENCE_MS", "0"))

    def synthesize_mp3(self, text: str, leading_silence_ms: int = 0) -> bytes:
        
        logging.info(
            "Azure Speech text_to_speech called: text_length=%s leading_silence_ms=%s text_preview=%s",
            len(text),
            leading_silence_ms or self.default_leading_silence_ms,
            text[:200],
        )

        if not self.key or not self.region:
            raise ValueError("AZURE_SPEECH_KEY and AZURE_SPEECH_REGION are required")
        token_resp = requests.post(
            f"https://{self.region}.api.cognitive.microsoft.com/sts/v1.0/issueToken",
            headers={"Ocp-Apim-Subscription-Key": self.key, "Content-Length": "0"},
            timeout=15,
        )

        logging.info("Azure Speech token acquired successfully")
        
        token_resp.raise_for_status()
        effective_leading_silence_ms = max(0, int(leading_silence_ms or self.default_leading_silence_ms))
        lead_in = f'<break time="{effective_leading_silence_ms}ms"/>' if effective_leading_silence_ms else ''
        ssml = f'''<speak version="1.0" xml:lang="en-US"><voice name="{self.voice}">{lead_in}{text}</voice></speak>'''
        resp = requests.post(
            f"https://{self.region}.tts.speech.microsoft.com/cognitiveservices/v1",
            headers={
                "Authorization": f"Bearer {token_resp.text}",
                "Content-Type": "application/ssml+xml",
                "X-Microsoft-OutputFormat": "audio-24khz-48kbitrate-mono-mp3",
                "User-Agent": "agility-standup",
            },
            data=ssml.encode("utf-8"),
            timeout=30,
        )

        logging.info(
            "Azure Speech TTS response: status_code=%s content_length=%s response_preview=%s",
            resp.status_code,
            len(resp.content or b""),
            resp.text[:500] if hasattr(resp, "text") else "",
        )

        resp.raise_for_status()
        return resp.content
