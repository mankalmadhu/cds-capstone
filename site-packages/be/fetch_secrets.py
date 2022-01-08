import requests
import streamlit as st


def fetch_doppler_secrets():
    url = "https://api.doppler.com/v3/configs/config/secrets/download?project=mlops&config=prd_mlops&format=env-no-quotes"

    headers = {"Accept": "application/json"}

    response = requests.request("GET", url, headers=headers)

    print(response.text)


def fetch_twitter_secrets():
    return st.secrets.twitter_credentials


if __name__ == "__main__":
    print(fetch_twitter_secrets())