# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                      |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| em\_workflows/brt/config.py               |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/config.py                   |       47 |       10 |        8 |        4 |     75% |20-37, 52, 75->74, 86->85, 91, 94->93, 95 |
| em\_workflows/constants.py                |       11 |        2 |        4 |        2 |     73% |     9, 11 |
| em\_workflows/dm\_conversion/config.py    |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/constants.py |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/flow.py      |      113 |        6 |       36 |       10 |     89% |17->16, 30->29, 49->48, 68-69, 102->101, 118->117, 136-137, 175-176, 215->221, 215->exit |
| em\_workflows/enums.py                    |       11 |        0 |        0 |        0 |    100% |           |
| em\_workflows/file\_path.py               |      109 |       12 |       22 |        9 |     84% |66->65, 75->74, 93-94, 113-114, 142, 228-229, 239->238, 250->254, 255-257, 261-262 |
| em\_workflows/lrg\_2d\_rgb/config.py      |        3 |        0 |        0 |        0 |    100% |           |
| em\_workflows/lrg\_2d\_rgb/constants.py   |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/lrg\_2d\_rgb/flow.py        |       70 |        0 |        8 |        5 |     94% |22->21, 45->44, 92->91, 130->136, 130->exit |
| em\_workflows/sem\_tomo/config.py         |        7 |        0 |        0 |        0 |    100% |           |
| em\_workflows/utils/utils.py              |      268 |      108 |      108 |       25 |     56% |61->60, 71-101, 105->104, 119->118, 125-126, 130->129, 160->162, 171->170, 180, 256, 285->287, 298-309, 328->327, 350-379, 510->509, 549->548, 567->578, 569, 579, 586->585, 592-597, 673-687, 699-733, 750, 754-769, 790->789, 801->803, 804, 810->809, 940->939, 979-995 |
|                                 **TOTAL** |  **657** |  **138** |  **186** |   **55** | **73%** |           |


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