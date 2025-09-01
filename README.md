# FRYK_production_management

## Introduction
노션에서 프로젝트를 관리하고 Google Sheets에서 결산을 작성하며 생산 관리를 하고 있습니다.


__airflow를 사용하는 이유는 다음과 같습니다.__
1. 노션에서 관리하는 프로젝트 별로 결산 시트가 생성되어야 함
2. 노션에 작성한 프로젝트 페이지의 값이 결산 시트에도 똑같이 쓰이는 값들이 있음.

![architecture](https://github.com/ImJoongHyeon/FRYK_production_management/blob/main/architecture.png)
--------------------------------------------------------------------------------------------------

## Version
- airflow : 3.0.1
- python : 3.10.17
- Notion API : 2022-02-22
    - reference : <https://developers.notion.com/reference/intro>
- Google Sheets API : v4
    - reference : <https://developers.google.com/workspace/sheets/api/reference/rest?hl=ko>

------------------------------------------------------------------------------------------

## SNS
블로그 : <https://outofanjoong.tistory.com/category/%ED%94%84%EB%A1%9C%EC%A0%9D%ED%8A%B8>
