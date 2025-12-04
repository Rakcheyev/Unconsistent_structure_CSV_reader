from core.analysis.column_profiler import profile_file_columns


def test_profile_file_columns_basic(tmp_path):
    path = tmp_path / "people.csv"
    path.write_text("name,age\nAlice,30\nBob,45\n", encoding="utf-8")

    profiles = profile_file_columns(path, delimiter=",", encoding="utf-8", errors="strict")

    assert len(profiles) == 2
    age_profile = profiles[1]
    assert age_profile.header == "age"
    assert age_profile.numeric_min == 30.0
    assert age_profile.numeric_max == 45.0
    assert age_profile.unique_estimate >= 2
    assert age_profile.type_distribution["integer"] == 2


def test_profile_file_columns_dates_and_nulls(tmp_path):
    path = tmp_path / "events.csv"
    path.write_text("id,event_date\n1,2024-01-01\n2,\n3,02.02.2024\n", encoding="utf-8")

    profiles = profile_file_columns(path, delimiter=",", encoding="utf-8", errors="strict")

    assert len(profiles) == 2
    date_profile = profiles[1]
    assert date_profile.null_count == 1
    assert date_profile.date_min == "2024-01-01"
    assert date_profile.date_max == "2024-02-02"
    assert date_profile.type_distribution["date"] == 2
