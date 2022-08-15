from fmg_packages.dto.mock_context_dto import MockContextDto
from fmg_packages.main import pull_data, cleanse_data, resulting_data, extract_results
from fmg_packages.utils.constants import DAG_RUN_KEY, CONF_REPORT_DATE_KEY

if __name__ == '__main__':
    process_date = '20220814T203800'
    params = {DAG_RUN_KEY: MockContextDto(conf={CONF_REPORT_DATE_KEY: process_date})}
    pull_data(**params)
    cleanse_data(**params)
    resulting_data(**params)
    extract_results(**params)
