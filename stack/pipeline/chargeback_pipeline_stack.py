import aws_cdk as cdk
from constructs import Construct
from aws_cdk import aws_codebuild as codebuild
from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_codepipeline_actions as codepipeline_actions
from aws_cdk import (aws_ec2 as ec2_,aws_lambda,aws_iam as iam_,aws_s3 as s3)
from stack.stack_stage.configuration import  RawConfig
from stack.stack_stage.application_stage import ChargebackStacks



class ChargebackPipelineStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, raw_config: RawConfig, **kwargs):
        super().__init__(scope, id, **kwargs)
        self._raw_config = raw_config
        source_output = codepipeline.Artifact()
        pipeline_name='Chargeback-Pipeline'
        
        #Github details
        connection_arn = self._raw_config.application['connection_arn']
        owner=self._raw_config.application['owner']
        git_repo=self._raw_config.application['repo_name']
        git_branch=self._raw_config.application['branch']
        
        security_group_id = self._raw_config.development.common_stack_info["security_group_id"]
        #Security group
        security_group = ec2_.SecurityGroup.from_security_group_id(self, "Chargeback Default Security Group",
                                                                       security_group_id=security_group_id,
                                                                       mutable=False)
        #vpc
        vpc=ec2_.Vpc.from_vpc_attributes(
            self, 'ChargebackVPC',
            vpc_id = self._raw_config.development.common_stack_info["vpc_id"],
            availability_zones = self._raw_config.development.common_stack_info["availability_zones"],
            private_subnet_ids = self._raw_config.development.common_stack_info["private_subnet_ids"],
        )

        #Pipeline s3 artifact
        pipeline_artifact_bucket = s3.Bucket(
                    self,
                    "Chargeback_Pipeline_Artifact_Bucket",
                    bucket_name="chargeback-pipeline-artifacts",
                    removal_policy=cdk.RemovalPolicy.DESTROY,
                    auto_delete_objects=True,
                )

        #Code-build image details
        code_build_environment =codebuild.BuildEnvironment(build_image=codebuild.LinuxBuildImage.STANDARD_5_0, 
                                                            compute_type=codebuild.ComputeType.SMALL, 
                                                            privileged = True)
        application_deploy = codebuild.PipelineProject(self, 'ChargebackDeployment',
                                                          project_name="Chargeback-Deployment",
                                                          vpc=vpc,
                                                          security_groups=[security_group],
                                                          environment=code_build_environment,
                                                          timeout=cdk.Duration.hours(2),
                                                          build_spec=codebuild.BuildSpec.from_source_filename(
                                                              "stack/pipeline/buildspec/pipeline_deploy_buildspec.yaml")
                                                        )
        #Pipeline_update
        pipeline_update = codebuild.PipelineProject(
                                                    self,
                                                    "Chargeback_Pipeline_Update",
                                                    project_name="Chargeback_Pipeline_Update",
                                                    vpc=vpc,
                                                    security_groups=[security_group],
                                                    environment =code_build_environment,
                                                    timeout = cdk.Duration.hours(2),
                                                    build_spec=codebuild.BuildSpec.from_source_filename("stack/pipeline/buildspec/pipeline_update_buildspec.yaml")
                                                )
        #Role/policy statements
        statement = iam_.PolicyStatement()
        statement.add_actions("ssm:Describe*")
        statement.add_actions("ssm:Get*")
        statement.add_actions("ssm:List*")
        statement.add_actions("ssm:DescribeParameters")
        statement.add_actions("ses:SendEmail")
        statement.add_actions("ses:SendRawEmail") 
        statement.add_actions("states:DescribeStateMachine")
        statement.add_actions("states:DescribeExecution")
        statement.add_actions("states:StartExecution")
        statement.add_actions("cloudformation:*")
        statement.add_actions("s3:*")
        statement.add_actions("ec2:*")
        statement.add_actions("apigateway:*")
        statement.add_actions("sts:*")    
        statement.add_actions("secretsmanager:*")
        statement.add_actions("iam:*")
        statement.add_actions("lambda:*")
        statement.add_actions("states:*")
        statement.add_actions("dynamodb:*")
        statement.add_resources("*")
        pipeline_update.add_to_role_policy(statement)
        
        #Pipeline definition
        pipeline = codepipeline.Pipeline(self, pipeline_name,
                                                pipeline_name=pipeline_name,
                                                restart_execution_on_update=True,
                                                artifact_bucket=pipeline_artifact_bucket
                                                )
        #Pipeline source stage
        pipeline.add_stage(
            stage_name="Source",
            actions=[
                codepipeline_actions.CodeStarConnectionsSourceAction(
                    action_name="GitHub",
                    connection_arn=connection_arn,
                    owner= owner,
                    repo=git_repo,
                    branch=git_branch,
                    code_build_clone_output=True,
                    output=source_output)])

        #Self-mutate stage   
        self_mutate_stage = pipeline.add_stage(stage_name="Pipeline_Update",actions=[
                                    codepipeline_actions.CodeBuildAction(
                                            action_name="Self_Mutate",
                                            project=pipeline_update,
                                            input=source_output
                                        )
                                ])
        # Deployment stage
        pipeline.add_stage(
            stage_name="Deployment",
            actions=[
                codepipeline_actions.CodeBuildAction(
                    action_name="Application_Deployment",
                    project=application_deploy,
                    input=source_output,
                )
            ]
        )
        app_env = cdk.Environment(**self._raw_config.development.env)
        app = ChargebackStacks(self,id = "development", env=app_env, raw_config=self._raw_config.development)
