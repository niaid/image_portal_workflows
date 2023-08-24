Workflows related to project previously referred to as "Hedwig".

Please refer to Spinx Docs below for _Installation_, _Local Set-up_, _Sending Pull Requests_, and _Testing_.

_Sphinx Docs_: https://niaid.github.io/image_portal_workflows/

Build/Test:

.. image:: https://github.com/mbopfNIH/image_portal_workflows/actions/workflows/main.yml/badge.svg?branch=main
    :target: https://github.com/mbopfNIH/image_portal_workflows/actions/workflows/main.yml/badge.svg?branch=main
    :alt: GitHub Action

Test Coverage:

.. image:: ../../coverage.svg

#### Docker

The Dockerfile is currently using,\
- bioformats2raw version: 0.7.0.
- imod version: 4.11.24

In order to build the docker image, use `--platform linux/amd64` option.
Explanation can be found [here](https://teams.microsoft.com/l/entity/com.microsoft.teamspace.tab.wiki/tab::5f55363b-bb53-4e5b-9564-8bed5289fdd5?context=%7B%22subEntityId%22%3A%22%7B%5C%22pageId%5C%22%3A15%2C%5C%22sectionId%5C%22%3A17%2C%5C%22origin%5C%22%3A2%7D%22%2C%22channelId%22%3A%2219%3A869be6677ee54848bc13f2066d847cc0%40thread.skype%22%7D&tenantId=14b77578-9773-42d5-8507-251ca2dc2b06)

```bash
$ docker build . -t hedwig_pipelines --platform linux/amd64
```
