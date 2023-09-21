# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                      |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| em\_workflows/\_\_init\_\_.py             |        0 |        0 |        0 |        0 |    100% |           |
| em\_workflows/brt/config.py               |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/config.py                   |       48 |       19 |       12 |        3 |     53% |26-43, 54-59, 81->80, 82-87, 90->89, 95, 98->97, 99 |
| em\_workflows/constants.py                |        8 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/config.py    |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/constants.py |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/file\_path.py               |      109 |       80 |       22 |        3 |     24% |20, 47-56, 59, 66->65, 72, 75->74, 82, 91-95, 104-105, 112-119, 138-146, 155-161, 170-172, 194-208, 223-231, 235-236, 239->238, 245-263 |
| em\_workflows/sem\_tomo/config.py         |        7 |        0 |        0 |        0 |    100% |           |
| em\_workflows/utils/utils.py              |      268 |      213 |      108 |       13 |     18% |27->exit, 41-57, 61->60, 71-101, 105->104, 113-115, 119->118, 125-126, 130->129, 160-167, 171->170, 179-184, 242-288, 298-309, 320-324, 328->327, 350-379, 503-506, 510->509, 523, 549->548, 562-582, 586->585, 592-597, 669-688, 699-733, 746-770, 780-786, 790->789, 801-806, 810->809, 816-822, 940->939, 953-973 |
|                                 **TOTAL** |  **453** |  **312** |  **142** |   **19** | **27%** |           |


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