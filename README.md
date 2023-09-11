# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                      |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| em\_workflows/brt/config.py               |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/config.py                   |       48 |       12 |       10 |        4 |     72% |18-35, 50, 81->80, 94->93, 95-96, 99->98, 100-101 |
| em\_workflows/constants.py                |        2 |        0 |        0 |        0 |    100% |           |
| em\_workflows/czi/config.py               |        3 |        0 |        0 |        0 |    100% |           |
| em\_workflows/czi/constants.py            |       11 |        2 |        4 |        2 |     73% |    11, 13 |
| em\_workflows/czi/flow.py                 |       83 |       48 |       24 |        3 |     36% |22-24, 28-41, 45-84, 88->87, 97-121, 125->124, 126-131, 134->exit |
| em\_workflows/dm\_conversion/config.py    |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/constants.py |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/flow.py      |      112 |        6 |       36 |       10 |     89% |17->16, 30->29, 49->48, 68-69, 102->101, 118->117, 136-137, 175-176, 215->221, 215->exit |
| em\_workflows/file\_path.py               |      109 |       12 |       22 |        9 |     84% |67->66, 76->75, 94-95, 114-115, 143, 230-231, 241->240, 252->256, 257-259, 263-264 |
| em\_workflows/lrg\_2d\_rgb/config.py      |        3 |        0 |        0 |        0 |    100% |           |
| em\_workflows/lrg\_2d\_rgb/constants.py   |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/lrg\_2d\_rgb/flow.py        |       68 |        0 |        8 |        5 |     93% |21->20, 44->43, 88->87, 122->128, 122->exit |
| em\_workflows/sem\_tomo/config.py         |        7 |        0 |        0 |        0 |    100% |           |
| em\_workflows/utils/utils.py              |      268 |      105 |      108 |       22 |     58% |60->59, 70-100, 104->103, 118->117, 124-125, 129->128, 159->161, 170->169, 179, 255, 284->286, 297-308, 327->326, 349-378, 509->508, 548->547, 566->577, 568, 585->584, 591-596, 672-686, 698-732, 753-768, 789->788, 800->802, 809->808, 943->942, 982-998 |
|                                 **TOTAL** |  **732** |  **185** |  **212** |   **55** | **69%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/niaid/image_portal_workflows/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/niaid/image_portal_workflows/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Fniaid%2Fimage_portal_workflows%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.