# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                      |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| em\_workflows/brt/config.py               |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/config.py                   |       47 |       10 |        8 |        4 |     75% |20-37, 52, 75->74, 86->85, 91, 94->93, 95 |
| em\_workflows/constants.py                |        2 |        0 |        0 |        0 |    100% |           |
| em\_workflows/czi/config.py               |        3 |        0 |        0 |        0 |    100% |           |
| em\_workflows/czi/constants.py            |       11 |        2 |        4 |        2 |     73% |    11, 13 |
| em\_workflows/czi/flow.py                 |       84 |       48 |       24 |        3 |     36% |22-24, 28-41, 45-84, 88->87, 97-121, 125->124, 126-131, 134->exit |
| em\_workflows/dm\_conversion/config.py    |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/constants.py |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/flow.py      |      113 |        6 |       36 |       10 |     89% |17->16, 30->29, 49->48, 68-69, 102->101, 118->117, 136-137, 175-176, 215->221, 215->exit |
| em\_workflows/enums.py                    |       11 |        0 |        0 |        0 |    100% |           |
| em\_workflows/file\_path.py               |      109 |       12 |       22 |        9 |     84% |66->65, 75->74, 93-94, 113-114, 142, 229-230, 240->239, 251->255, 256-258, 262-263 |
| em\_workflows/lrg\_2d\_rgb/config.py      |        3 |        0 |        0 |        0 |    100% |           |
| em\_workflows/lrg\_2d\_rgb/constants.py   |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/lrg\_2d\_rgb/flow.py        |       69 |        0 |        8 |        5 |     94% |21->20, 44->43, 88->87, 122->128, 122->exit |
| em\_workflows/sem\_tomo/config.py         |        7 |        0 |        0 |        0 |    100% |           |
| em\_workflows/utils/utils.py              |      268 |      105 |      108 |       22 |     58% |60->59, 70-100, 104->103, 118->117, 124-125, 129->128, 159->161, 170->169, 179, 255, 284->286, 297-308, 327->326, 349-378, 509->508, 548->547, 566->577, 568, 585->584, 591-596, 672-686, 698-732, 753-768, 789->788, 800->802, 809->808, 943->942, 982-998 |
|                                 **TOTAL** |  **745** |  **183** |  **210** |   **55** | **70%** |           |


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