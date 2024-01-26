# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/niaid/image_portal_workflows/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                      |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| em\_workflows/\_\_init\_\_.py             |        0 |        0 |        0 |        0 |    100% |           |
| em\_workflows/brt/config.py               |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/config.py                   |       58 |       11 |       10 |        3 |     79% |44-71, 106->105, 115->114, 120, 123->122, 124 |
| em\_workflows/constants.py                |        9 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/config.py    |        4 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/constants.py |       10 |        0 |        0 |        0 |    100% |           |
| em\_workflows/dm\_conversion/flow.py      |      112 |       51 |       38 |        8 |     50% |27->23, 37, 44->40, 65->61, 80-112, 119->115, 127-131, 138->134, 143-200, 217->203, 286 |
| em\_workflows/file\_path.py               |      131 |       22 |       30 |        8 |     79% |61, 68->67, 77->76, 95-96, 115-116, 140-148, 172-174, 239-240, 260, 275->274, 291-293 |
| em\_workflows/lrg\_2d\_rgb/config.py      |        3 |        0 |        0 |        0 |    100% |           |
| em\_workflows/lrg\_2d\_rgb/constants.py   |        5 |        0 |        0 |        0 |    100% |           |
| em\_workflows/lrg\_2d\_rgb/flow.py        |       89 |       33 |       18 |        8 |     62% |24->23, 54->50, 61, 68->64, 69-70, 74->73, 75-77, 84->80, 89-108, 112->111, 113-147, 165->150, 210, 216-217 |
| em\_workflows/sem\_tomo/config.py         |        7 |        0 |        0 |        0 |    100% |           |
| em\_workflows/utils/neuroglancer.py       |       57 |       34 |       18 |        4 |     36% |13-17, 38-67, 105, 107, 109, 112, 121-126, 130-135 |
| em\_workflows/utils/utils.py              |      285 |      117 |       98 |       22 |     55% |49-63, 89->85, 100-130, 134->133, 149->148, 155-156, 160->159, 190-197, 202->201, 211, 252, 294-305, 327->323, 349-379, 384->383, 400->399, 412->411, 432, 449->448, 455-460, 464->463, 473-490, 551-575, 595->594, 609, 614, 625->618, 642->641, 662-678, 705-713 |
|                                 **TOTAL** |  **775** |  **268** |  **212** |   **53** | **61%** |           |


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