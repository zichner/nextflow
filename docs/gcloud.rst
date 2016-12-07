.. _amazonscloud-page:

************
Google Cloud
************

Storage
=======

Nextflow includes a built in support for `Google Cloud Storage <https://cloud.google.com/storage/>`_. Files stored in a Google Cloud bucket can be
accessed transparently in your pipeline script like any other file in the local file system.

Google storage path
--------------------
In order to access a file store in the Google Cloud storage you only need to prefix the file path with
the ``gs`` schema and the `bucket` name where it is stored.

For example if you need to access the file ``/data/sequences.fa`` stored in a bucket with name ``my-bucket``,
that file can be accessed using the following fully qualified path::

   gs://my-bucket/data/sequences.fa


The usual file operations can be applied on a path handle created using the above notation. For example the
content of a file can be printed as shown below::

    println file('gs://my-bucket/data/sequences.fa').text


or the content of a bucket can be listed with the following snippet::

    file('gs://my-bucket/').list().each { println it }


See section :ref:`script-file-io` to learn more about available file operations.


Security credentials
---------------------

The Google Cloud credentials and project ID can be specified in the `nextflow.config` file as
shown below::

    google {
        projectId = 'your-project-id'
        credentials = '/path/to/your/credentials/file'
    }