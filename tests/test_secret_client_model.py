from mainsequence.client.models_foundry import Secret


def test_secret_deserializes_public_uid_payload():
    secret = Secret(
        uid="498d499f-b74c-43f7-acf1-2e2955ad0e6b",
        name="OPENFIGI_API_KEY",
    )

    assert secret.uid == "498d499f-b74c-43f7-acf1-2e2955ad0e6b"
    assert secret.name == "OPENFIGI_API_KEY"
