# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                      |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| em\_workflows/\_\_init\_\_.py             |        0 |        0 |        0 |        0 |    100% |           |
| em\_workflows/brt/config.py               |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/config.py                   |       48 |       12 |       12 |        3 |     72% |26-43, 54-59, 81->80, 90->89, 98->97, 99 |
| em\_workflows/constants.py                |        9 |        0 |        0 |        0 |    100% |           |
| em\_workflows/czi/config.py               |        3 |        0 |        0 |        0 |    100% |           |
| em\_workflows/czi/constants.py            |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/czi/flow.py                 |       89 |       50 |       30 |        4 |     36% |21-37, 41-83, 87->86, 88-98, 102->101, 103-107, 111->110, 112-120, 123->exit |
| em\_workflows/dm\_conversion/config.py    |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/constants.py |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/flow.py      |      113 |       23 |       36 |       12 |     75% |17->16, 30->29, 49->48, 67-98, 102->101, 110-114, 118->117, 128, 132, 136-137, 175-176, 183->189, 183->exit |
| em\_workflows/file\_path.py               |      109 |       12 |       22 |        9 |     82% |66->65, 75->74, 93-94, 113-114, 141-143, 230-231, 241->240, 252->256, 257-259 |
| em\_workflows/sem\_tomo/config.py         |        7 |        0 |        0 |        0 |    100% |           |
| em\_workflows/utils/neuroglancer.py       |       35 |       26 |       12 |        0 |     19% |10-12, 34-68, 72-77 |
| em\_workflows/utils/utils.py              |      268 |      120 |      108 |       23 |     52% |43-57, 61->60, 71-101, 105->104, 119->118, 125-126, 130->129, 160->162, 163, 171->170, 221, 250->252, 263-274, 293->292, 315-344, 379->378, 396->395, 414->425, 416, 426, 433->432, 439-444, 476-490, 502-536, 557-572, 593->592, 604->606, 613->612, 629->628, 646-662 |
|                                 **TOTAL** |  **698** |  **243** |  **220** |   **51** | **60%** |           |


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