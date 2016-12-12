.. _amazonscloud-page:

************
Google Cloud
************

Storage
=======

Nextflow includes a built in support for `Google Cloud Storage <https://cloud.google.com/storage/>`_. Files stored
in a Google Cloud bucket can be accessed transparently in your pipeline script like any other file in the local file system.

In order to access a file stored in the Google storage you only need to prefix the file path with the ``gs`` schema
and specify the `bucket` name where it stored.

For example, if you need to access the file ``/data/sequences.fa`` stored in a bucket named ``my-bucket``,
that file can be accessed using the following fully qualified path::

   gs://my-bucket/data/sequences.fa


The usual file operations can be applied on a path handle created by using the above notation. For example the
content of a file can be printed as shown below::

    println file('gs://my-bucket/data/sequences.fa').text


or the content of a bucket can be listed with the following code snippet::

    file('gs://my-bucket/').list().each { println it }


See section :ref:`script-file-io` to learn more about available file operations.


Access credentials
==================

When using Nextflow from within a Google Compute instance, no additional authentication steps are necessary.


In all other cases it is required to specify a `Google service account key <https://cloud.google.com/storage/docs/authentication?hl=en#service_accounts>`_
and project ID in the ``nextflow.config`` file as shown below::

    google {
        projectId = 'your-project-id'
        credentials = '/path/to/your/credentials/file.json'
    }


If these information are not provide in the configuration file, Nextflow will look for the following
environment variables::

    GOOGLE_PROJECT_ID=<your project id>
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credentials/file.json



.. warning:: To access Google Cloud services, you need to ensure that the necessary Google Cloud APIs are enabled
  for your project. To do this, follow the instructions on the `authentication document <https://github.com/GoogleCloudPlatform/gcloud-common/blob/master/authentication/readme.md#authentication>`_.
