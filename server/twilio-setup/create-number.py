#!/usr/bin/env python3
"""
Twilio phone number setup helper
1. Creates a Twilio phone number (US toll-free for best quality, OR local).
2. Sets the Voice webhook to your Render app.
3. Optionally prints the number for your tests.

Usage:
  python create-twilio-phone-number.py
"""

import os
import sys
from twilio.rest import Client


def setup():
    sid = os.environ.get("TWILIO_ACCOUNT_SID")
    token = os.environ.get("TWILIO_AUTH_TOKEN")
    public_url = os.environ.get("PUBLIC_WS_URL", "").replace("wss://", "https://")

    if not all([sid, token, public_url]):
        print("ERROR: Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, PUBLIC_WS_URL first.")
        sys.exit(1)

    client = Client(sid, token)

    voice_url = f"{public_url}/twilio/voice"
    print(f"Configuring Twilio webhook → {voice_url}")

    # Look for available numbers
    numbers = client.available_phone_numbers("US").local.list(
        limit=10,
        sms_enabled=False,
        voice_enabled=True,
    )
    if not numbers:
        print("No local numbers available, trying toll-free…")
        numbers = client.available_phone_numbers("US").toll_free.list(limit=10)

    if not numbers:
        print("SOLD OUT. Buy a number in the Twilio Console: https://console.twilio.com")
        sys.exit(1)

    picked = numbers[0]
    print(f"Buying: {picked.friendly_name} ({picked.phone_number})…")

    incoming = client.incoming_phone_numbers.create(
        phone_number=picked.phone_number,
        voice_url=voice_url,
        voice_method="POST",
    )

    print(f"\n✅ Done!")
    print(f"   Phone number : {incoming.phone_number}")
    print(f"   Voice webhook: {incoming.voice_url}")
    print(f"\nCall this number to test your AI agent.")
    print(f"Change the URL later with: twilio phone-numbers:update {incoming.sid} --voice-url <url>")
    return incoming.phone_number


if __name__ == "__main__":
    setup()
