from airflow.decorators import dag, task
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from include.database.insert_scraping import insert_into_postgres
import random
import requests
import time

ROOT = "https://www.welcometothejungle.com"
WEBSITE = f"{ROOT}/fr/pages/emploi-data-engineer-paris-75000"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    'Cache-Control': 'no-cache'
}

@dag(
    schedule_interval="@once",
    start_date=datetime(2025, 3, 7),
    catchup=False,
    tags=["scraping", "beautifulsoup"]
)
def beautifulsoup_dag():
    
    @task()
    def get_jobs_urls():
        """Get all jobs URLs on all pages"""
        response = requests.get(WEBSITE, headers=HEADERS)
        soup = BeautifulSoup(response.text, "lxml")
        pagination = soup.find("nav", {"role": "navigation"}).find_all("li")
        last_page = pagination[-2].text
        jobs_urls = []

        # for page in range(1, int(last_page)+1): #BUG on the website, the content of the first page is always sent
        for page in range(1, 2):
            website = f'{WEBSITE}?page={page}'
            result = requests.get(website, headers=HEADERS)
            soup = BeautifulSoup(result.text, "lxml")
            page_jobs_list = soup.find("ul", {"kind": "jobs"}).find_all("li")
            date_min = datetime.today().date() - timedelta(days=30)
            page_jobs = [f"{ROOT}{job.find("a")["href"]}" for job in page_jobs_list if datetime.fromisoformat(job.find("time")["datetime"].replace("Z", "")).date() >= date_min]
            jobs_urls += page_jobs
            time.sleep(random.uniform(3, 7))

        return jobs_urls

    @task(pool="scraping_pool")
    def scrape_job(job_url):
        """Collect the job data and store it in a database"""
        print(job_url)
        response = requests.get(job_url, headers=HEADERS)
        soup = BeautifulSoup(response.text, "lxml")

        job_metadata = soup.find("div", {"data-testid": "job-metadata-block"})
        date_posted_str = job_metadata.find("time")["datetime"]
        date_posted = datetime.fromisoformat(date_posted_str.replace("Z", "")).date()
        title = job_metadata.find("h2").get_text(strip=True)
        company = job_metadata.find("div").find("span").get_text(strip=True)
        location = job_metadata.find("i", {"name": "location"}).find_next_sibling().find("span", text=True).get_text(strip=True)

        job_description = ""
        job_section = soup.find("div", {"data-testid": "job-section-description"})
        if job_section:
            job_description_list = job_section.find("div", {"data-is-view-more": True}).find_all(text=True)
            job_description = "\n".join([l.get_text(strip=True) for l in job_description_list])

        profile_searched = ""
        profile_section = soup.find("div", {"data-testid": "job-section-experience"})
        if profile_section:
            profile_searched_list = profile_section.find("div", {"data-is-view-more": True}).find_all(text=True)
            profile_searched = "\n".join([l.get_text(strip=True) for l in profile_searched_list])

        insert_into_postgres((date_posted, title, company, location, job_description, profile_searched, job_url))
        time.sleep(random.uniform(3, 7))

        return job_url
    
    job_urls = get_jobs_urls()
    scrape_job.expand(job_url=job_urls)

beautifulsoup_dag()
