import boto3
import json

REGION = 'us-east-1'
KEY_NAME = 'vockey'
IAM_PROFILE = 'LabInstanceProfile'
INSTANCE_TYPE = 't2.large'

def crear_infraestructura():
    print("[Hito] Iniciando la creación de infraestructura para el Clúster...")
    
    ec2_client = boto3.client('ec2', region_name=REGION)
    ec2_resource = boto3.resource('ec2', region_name=REGION)
    sg_name = 'kafka-flink-cluster-sg'
    
    try:
        response = ec2_client.create_security_group(GroupName=sg_name, Description='SG para el cluster')
        sg_id = response['GroupId']
        ec2_client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                {'IpProtocol': 'tcp', 'FromPort': 8081, 'ToPort': 8081, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                {'IpProtocol': '-1', 'UserIdGroupPairs': [{'GroupId': sg_id}]}
            ]
        )
    except ec2_client.exceptions.ClientError as e:
        if 'InvalidGroup.Duplicate' in str(e):
            sgs = ec2_client.describe_security_groups(GroupNames=[sg_name])
            sg_id = sgs['SecurityGroups'][0]['GroupId']
        else:
            raise e

    ssm_client = boto3.client('ssm', region_name=REGION)
    ami_response = ssm_client.get_parameter(Name='/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id')
    ami_id = ami_response['Parameter']['Value']

    print(f"[Hito] Lanzando 5 instancias EC2 ({INSTANCE_TYPE})...")
    instances = ec2_resource.create_instances(
        ImageId=ami_id,
        MinCount=5, MaxCount=5,
        InstanceType=INSTANCE_TYPE,
        KeyName=KEY_NAME,
        SecurityGroupIds=[sg_id],
        IamInstanceProfile={'Name': IAM_PROFILE},
        BlockDeviceMappings=[{'DeviceName': '/dev/sda1', 'Ebs': {'VolumeSize': 20, 'VolumeType': 'gp2', 'DeleteOnTermination': True}}],
        TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Proyecto', 'Value': 'Kafka-Flink-Cluster'}]}]
    )

    print("[Hito] Esperando a que las instancias inicien correctamente...")
    for instance in instances:
        instance.wait_until_running()
        instance.reload() 
    
    roles = ['Maestro', 'Trabajador1', 'Trabajador2', 'Trabajador3', 'Trabajador4']
    nodos_info = {}
    
    for i, instance in enumerate(instances):
        rol = roles[i]
        instance.create_tags(Tags=[{'Key': 'Rol', 'Value': rol}, {'Key': 'Name', 'Value': f'Nodo-{rol}'}])
        nodos_info[rol] = {'id': instance.id, 'ip_publica': instance.public_ip_address, 'ip_privada': instance.private_ip_address}
    
    with open('nodos_info.json', 'w') as f:
        json.dump(nodos_info, f, indent=4)
        
    print("[Hito] ¡Infraestructura desplegada y archivo nodos_info.json generado!")

if __name__ == '__main__':
    crear_infraestructura()