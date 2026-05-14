"""Unit tests for the enrich.py post-rollup quality filters.

Covers _is_us_company, _is_garbage, _is_competitor — the three filters
that decide whether a rolled-up company appears in the dashboard.
"""
import pytest
from enrich import _is_us_company, _is_garbage, _is_competitor


@pytest.mark.parametrize("location,expected", [
    # US — keep
    ("New York, NY", True),
    ("San Francisco, CA", True),
    ("Boston, MA", True),
    ("Remote, United States", True),
    ("Austin, TX", True),
    ("Remote", True),
    ("", True),
    (None, True),
    # Non-US — drop
    ("Toronto", False),
    ("Toronto, ON, Canada", False),
    ("London, UK", False),
    ("Dublin, Ireland", False),
    ("Sydney, Australia", False),
    ("Bengaluru, Karnataka, India, APAC", False),
    ("Mexico City", False),
    ("Lisbon, Portugal", False),
    # Mixed signals — non-US wins (regression: previously slipped through)
    ("Remote, Canada", False),
    ("Remote - Toronto", False),
    ("Remote Canada", False),
])
def test_is_us_company(location, expected):
    assert _is_us_company({"location": location}) is expected


@pytest.mark.parametrize("name,url,expected", [
    # Content/aggregator sites
    ("Free Lunch", "https://www.levels.fyi/benefits/Free-Lunch", True),
    ("Office Food Perks", "https://www.businessinsider.com/foo", True),
    ("Glassdoor", "https://www.glassdoor.com/Benefits/foo", True),
    ("Built In NYC", "https://builtinnyc.com/foo", True),
    ("Strategy Business", "https://strategy-business.com/blog/foo", True),
    # Catering vendor sites
    ("#1 Corporate Lunch", "https://deborahmillercatering.com/dining/", True),
    ("Forkable", "https://www.forkable.com/", True),
    # Real companies — keep
    ("Robinhood", "https://boards.greenhouse.io/robinhood/123", False),
    ("Acme Corp", "https://jobs.lever.co/acme/abc", False),
])
def test_is_garbage(name, url, expected):
    assert _is_garbage({"company": name, "sample_url": url}) is expected


@pytest.mark.parametrize("name,expected", [
    # Core competitors and regional variants
    ("DoorDash", True),
    ("DoorDash USA", True),
    ("DoorDash Australia", True),
    ("DoorDash Mexico", True),
    ("Doordashusa", True),
    ("Grubhub", True),
    ("Grubhub Inc", True),
    ("Uber Eats", True),
    ("UberEats", True),
    ("Uber Eats for Business", True),
    ("Sharebite", True),
    ("Forkable", True),
    ("Caviar", True),
    ("Fooda", True),
    ("Zerocater", True),
    ("CookUnity", True),
    ("ezCater", True),
    # Word-boundary protection — should NOT match
    ("DoorDashing Inc", False),
    ("Hungrybox", False),  # "hungry" requires \b
    # Legit companies that mention competitors as keywords — keep
    ("Acme Corp", False),
    ("Robinhood Markets Inc", False),
    ("Click Therapeutics", False),
])
def test_is_competitor(name, expected):
    assert _is_competitor({"company": name}) is expected


def test_filters_independent():
    """A company can fail multiple filters; we should detect each one."""
    row = {
        "company": "DoorDash Australia",
        "location": "Sydney, NSW",
        "sample_url": "https://job-boards.greenhouse.io/doordashaustralia/jobs/1",
    }
    assert _is_competitor(row) is True
    assert _is_us_company(row) is False
    assert _is_garbage(row) is False
