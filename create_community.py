import requests
import json
import os

token=os.environ['TOKEN']

data = {
    "access": {
            "visibility": "public",
            "member_policy": "open",
            "record_policy": "open"
            },
    "slug": "tccon",
    "metadata": {
            "title": "Total Carbon Column Observing Network (TCCON)",
            "description": "TCCON is a network of ground-based Fourier Transform Spectrometers recording direct solar spectra in the near infrared spectral region. From these spectra, accurate and precise column-averaged abundances of CO2, CH4, N2O, HF, CO, H2O, and HDO are retrieved and reported here.",
            "website": "https://tccondata.org/",
    }
}


url = "https://data.caltechlibrary.dev/"

headers = {
            "Authorization": "Bearer %s" % token,
            "Content-type": "application/json",
        }

result = requests.post(
            url + "api/communities", headers=headers, json=data)
print(result.text)

