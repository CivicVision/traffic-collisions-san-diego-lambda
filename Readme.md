# San Diego Traffic Crash Data Pipeline

A AWS Lambda Pipeline for analysing the traffic crash data from the Open Data Portal of the City of San Diego.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Installing

This is a python based project so you need python (min 2.7)
Create a virtualenv for this folder, we call this `venv` but you can name it whatever you like, but you need to adjust the name in the `serverless.yml`

``
virtualenv venv
``

Install python packages with
``
pip install -r requirements.txt
``

If you want to deploy to AWS use serverless. Install it via npm or yarn.

## Deployment

You need to setup all the AWS Credentials and change the profile in the `serverless.yml` and then run 
Deploy with `serverless deploy -v`

## Built With

* agate
* serverless

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors

* **Mila Frerichs** - *Initial work* - [milafrerichs](https://github.com/milafrerichs)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Thanks for maksim for the inspiration on how to setup my lambda (Open Data Portal of San Diego)
* Thanks for the agate team for creating an easy to use lib
