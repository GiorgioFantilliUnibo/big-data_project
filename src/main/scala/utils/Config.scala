package utils

object Config {

  // The local directory containing this repository
  val projectDir :String = "/C:/Users/giorg/IdeaProjects/big-data_project"
  // The name of the shared bucket on AWS S3 to read datasets (so you don't need to upload them in your bucket)
  val s3sharedBucketName :String = "unibo-bd2526-egallinucci-shared"
  // The name of your bucket on AWS S3
  val s3bucketName :String = "unibo-bdp2526-gfantilli"
  // The path to the credentials file for AWS (if you follow instructions, this should not be updated)
  val credentialsPath :String = "/aws_credentials.txt"

}
