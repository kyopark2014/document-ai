"""VPC + endpoints for document-ai (ess-work / harness-work pattern).

Creates a dedicated VPC with public + private subnets, a single NAT Gateway,
S3/DynamoDB gateway endpoints, and interface endpoints for ECR, Logs,
Secrets Manager, and Bedrock (runtime + AgentCore).

Harness AgentCore Runtime and Lambda Managed Instances Capacity Provider
share the same private subnets.
"""

from __future__ import annotations

import ipaddress
import time
from typing import Dict, List, Optional, Set

from botocore.exceptions import ClientError

# Prefer a CIDR that does not collide with ess-work (10.20) / harness-work (10.52).
CANDIDATE_CIDRS = [
    "10.53.0.0/16",
    "10.54.0.0/16",
    "10.55.0.0/16",
    "10.56.0.0/16",
    "10.57.0.0/16",
    "172.30.0.0/16",
    "172.31.0.0/16",
]


class VpcNetworkProvisioner:
    def __init__(
        self,
        *,
        ec2_client,
        region: str,
        account_id: str,
        project_name: str,
        logger,
        lmi_sg_name: str,
    ):
        self.ec2 = ec2_client
        self.region = region
        self.account_id = account_id
        self.project_name = project_name
        self.logger = logger
        self.lmi_sg_name = lmi_sg_name

    def vpc_name(self) -> str:
        return f"vpc-for-{self.project_name}"

    def agent_runtime_sg_name(self) -> str:
        return f"agent-runtime-sg-for-{self.project_name}"

    def vpce_sg_name(self) -> str:
        return f"vpce-sg-for-{self.project_name}"

    # --- public API ----------------------------------------------------------

    def ensure_vpc(self) -> Dict[str, object]:
        """Create or reuse project VPC, endpoints, and workload security groups."""
        self.logger.info(f"Ensuring VPC for {self.project_name}: {self.vpc_name()}")
        existing = self._find_vpc_by_name(self.vpc_name())
        if existing:
            vpc_id = existing
            self.logger.info(f"  Reusing VPC: {vpc_id}")
            self._enable_vpc_dns(vpc_id)
            public_subnets, private_subnets = self._classify_subnets(vpc_id)
            if len(private_subnets) < 1:
                raise RuntimeError(
                    f"VPC {vpc_id} has no private subnets; "
                    "create private subnets or delete the VPC and re-run installer."
                )
            if public_subnets and private_subnets:
                self._ensure_nat_routing(vpc_id, public_subnets, private_subnets)
            vpc_info: Dict[str, object] = {
                "vpc_id": vpc_id,
                "public_subnets": public_subnets,
                "private_subnets": private_subnets,
            }
        else:
            vpc_info = self._create_vpc()

        vpc_id = str(vpc_info["vpc_id"])
        private_subnets = list(vpc_info["private_subnets"])
        self.ensure_private_subnet_vpc_endpoints(vpc_id, private_subnets)

        agent_sg = self.ensure_agent_runtime_security_group(vpc_id)
        lmi_sg = self.ensure_lmi_security_group(vpc_id)
        vpc_info["agent_runtime_security_group_id"] = agent_sg
        vpc_info["lmi_security_group_id"] = lmi_sg
        vpc_info["agent_runtime_security_groups"] = [agent_sg]
        vpc_info["lmi_security_groups"] = [lmi_sg]
        return vpc_info

    def ensure_agent_runtime_security_group(self, vpc_id: str) -> str:
        """Egress-all SG for AgentCore Harness in private subnets."""
        return self.create_security_group(
            vpc_id=vpc_id,
            group_name=self.agent_runtime_sg_name(),
            description=(
                f"AgentCore Harness runtime for {self.project_name} "
                "(egress via NAT / VPC endpoints)"
            ),
        )

    def ensure_lmi_security_group(self, vpc_id: str) -> str:
        """Egress-all SG for Lambda Managed Instances capacity provider."""
        return self.create_security_group(
            vpc_id=vpc_id,
            group_name=self.lmi_sg_name,
            description=(
                f"Outbound access for {self.project_name} Lambda Managed Instances"
            ),
        )

    def build_harness_runtime_environment(
        self, vpc_info: Dict[str, object]
    ) -> Dict:
        """CreateHarness/UpdateHarness environment with networkMode VPC."""
        return {
            "agentCoreRuntimeEnvironment": {
                "lifecycleConfiguration": {
                    "idleRuntimeSessionTimeout": 600,
                    "maxLifetime": 14400,
                },
                "networkConfiguration": {
                    "networkMode": "VPC",
                    "networkModeConfig": {
                        "subnets": list(vpc_info.get("private_subnets") or []),
                        "securityGroups": list(
                            vpc_info.get("agent_runtime_security_groups") or []
                        ),
                    },
                },
            }
        }

    def lmi_vpc_config(self, vpc_info: Dict[str, object]) -> Dict[str, List[str]]:
        """VpcConfig for Lambda Managed Instances capacity provider."""
        return {
            "SubnetIds": list(vpc_info.get("private_subnets") or []),
            "SecurityGroupIds": list(vpc_info.get("lmi_security_groups") or []),
        }

    # --- VPC create / classify -----------------------------------------------

    def _find_vpc_by_name(self, name: str) -> Optional[str]:
        resp = self.ec2.describe_vpcs(
            Filters=[{"Name": "tag:Name", "Values": [name]}]
        )
        vpcs = resp.get("Vpcs") or []
        return vpcs[0]["VpcId"] if vpcs else None

    def _enable_vpc_dns(self, vpc_id: str) -> None:
        self.ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
        self.ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})

    def _get_available_cidr_block(self) -> str:
        existing: Set[str] = set()
        try:
            for vpc in self.ec2.describe_vpcs().get("Vpcs") or []:
                existing.add(vpc["CidrBlock"])
                for assoc in vpc.get("CidrBlockAssociationSet") or []:
                    if assoc.get("CidrBlock"):
                        existing.add(assoc["CidrBlock"])
        except ClientError as e:
            self.logger.warning(f"  Could not list VPC CIDRs: {e}")
        for cidr in CANDIDATE_CIDRS:
            if cidr not in existing:
                self.logger.info(f"  Using CIDR block: {cidr}")
                return cidr
        self.logger.warning("  All candidate CIDRs in use; using 10.58.0.0/16")
        return "10.58.0.0/16"

    def _classify_subnets(self, vpc_id: str) -> tuple[List[str], List[str]]:
        subnets = self.ec2.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        ).get("Subnets", [])
        public_subnets: List[str] = []
        private_subnets: List[str] = []
        for subnet in subnets:
            if subnet.get("State") != "available":
                continue
            name = ""
            for tag in subnet.get("Tags") or []:
                if tag["Key"] == "Name":
                    name = tag["Value"]
                    break
            sid = subnet["SubnetId"]
            if "private" in name.lower():
                private_subnets.append(sid)
            elif "public" in name.lower():
                public_subnets.append(sid)
            elif self._subnet_is_public(sid):
                public_subnets.append(sid)
            else:
                private_subnets.append(sid)
        return public_subnets, private_subnets

    def _subnet_is_public(self, subnet_id: str) -> bool:
        rts = self.ec2.describe_route_tables(
            Filters=[{"Name": "association.subnet-id", "Values": [subnet_id]}]
        ).get("RouteTables", [])
        if not rts:
            subnet = self.ec2.describe_subnets(SubnetIds=[subnet_id])["Subnets"][0]
            rts = self.ec2.describe_route_tables(
                Filters=[
                    {"Name": "vpc-id", "Values": [subnet["VpcId"]]},
                    {"Name": "association.main", "Values": ["true"]},
                ]
            ).get("RouteTables", [])
        for rt in rts:
            for route in rt.get("Routes", []):
                if str(route.get("GatewayId", "")).startswith("igw-"):
                    return True
        return False

    def _create_vpc(self) -> Dict[str, object]:
        azs = [
            z["ZoneName"]
            for z in self.ec2.describe_availability_zones(
                Filters=[{"Name": "state", "Values": ["available"]}]
            )["AvailabilityZones"]
        ][:2]
        if len(azs) < 2:
            raise RuntimeError("Need at least 2 availability zones for document-ai VPC")

        cidr = self._get_available_cidr_block()
        network = ipaddress.ip_network(cidr)
        subnet_networks = list(network.subnets(new_prefix=24))

        vpc_id = self.ec2.create_vpc(
            CidrBlock=cidr,
            TagSpecifications=[
                {
                    "ResourceType": "vpc",
                    "Tags": [
                        {"Key": "Name", "Value": self.vpc_name()},
                        {"Key": "Project", "Value": self.project_name},
                    ],
                }
            ],
        )["Vpc"]["VpcId"]
        self.ec2.get_waiter("vpc_available").wait(VpcIds=[vpc_id])
        self._enable_vpc_dns(vpc_id)
        self.logger.info(f"  Created VPC: {vpc_id} ({cidr})")

        igw_id = self.ec2.create_internet_gateway(
            TagSpecifications=[
                {
                    "ResourceType": "internet-gateway",
                    "Tags": [
                        {"Key": "Name", "Value": f"igw-for-{self.project_name}"},
                        {"Key": "Project", "Value": self.project_name},
                    ],
                }
            ]
        )["InternetGateway"]["InternetGatewayId"]
        self.ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

        public_rt = self.ec2.create_route_table(
            VpcId=vpc_id,
            TagSpecifications=[
                {
                    "ResourceType": "route-table",
                    "Tags": [
                        {
                            "Key": "Name",
                            "Value": f"public-rt-for-{self.project_name}",
                        },
                        {"Key": "Project", "Value": self.project_name},
                    ],
                }
            ],
        )["RouteTable"]["RouteTableId"]
        self.ec2.create_route(
            RouteTableId=public_rt,
            DestinationCidrBlock="0.0.0.0/0",
            GatewayId=igw_id,
        )

        public_subnets: List[str] = []
        private_subnets: List[str] = []
        for i, az in enumerate(azs):
            pub_cidr = str(subnet_networks[i])
            priv_cidr = str(subnet_networks[i + 2])

            pub = self.ec2.create_subnet(
                VpcId=vpc_id,
                CidrBlock=pub_cidr,
                AvailabilityZone=az,
                TagSpecifications=[
                    {
                        "ResourceType": "subnet",
                        "Tags": [
                            {
                                "Key": "Name",
                                "Value": f"public-{i}-for-{self.project_name}",
                            },
                            {"Key": "aws-cdk:subnet-type", "Value": "Public"},
                            {"Key": "Project", "Value": self.project_name},
                        ],
                    }
                ],
            )["Subnet"]["SubnetId"]
            self.ec2.modify_subnet_attribute(
                SubnetId=pub, MapPublicIpOnLaunch={"Value": True}
            )
            self.ec2.associate_route_table(SubnetId=pub, RouteTableId=public_rt)
            public_subnets.append(pub)

            priv = self.ec2.create_subnet(
                VpcId=vpc_id,
                CidrBlock=priv_cidr,
                AvailabilityZone=az,
                TagSpecifications=[
                    {
                        "ResourceType": "subnet",
                        "Tags": [
                            {
                                "Key": "Name",
                                "Value": f"private-{i}-for-{self.project_name}",
                            },
                            {"Key": "aws-cdk:subnet-type", "Value": "Private"},
                            {"Key": "Project", "Value": self.project_name},
                        ],
                    }
                ],
            )["Subnet"]["SubnetId"]
            private_subnets.append(priv)

        eip = self.ec2.allocate_address(Domain="vpc")["AllocationId"]
        nat_id = self.ec2.create_nat_gateway(
            SubnetId=public_subnets[0],
            AllocationId=eip,
            TagSpecifications=[
                {
                    "ResourceType": "natgateway",
                    "Tags": [
                        {"Key": "Name", "Value": f"nat-for-{self.project_name}"},
                        {"Key": "Project", "Value": self.project_name},
                    ],
                }
            ],
        )["NatGateway"]["NatGatewayId"]
        self.logger.info(f"  Waiting for NAT Gateway: {nat_id}")
        self.ec2.get_waiter("nat_gateway_available").wait(NatGatewayIds=[nat_id])

        private_rt = self.ec2.create_route_table(
            VpcId=vpc_id,
            TagSpecifications=[
                {
                    "ResourceType": "route-table",
                    "Tags": [
                        {
                            "Key": "Name",
                            "Value": f"private-rt-for-{self.project_name}",
                        },
                        {"Key": "Project", "Value": self.project_name},
                    ],
                }
            ],
        )["RouteTable"]["RouteTableId"]
        self.ec2.create_route(
            RouteTableId=private_rt,
            DestinationCidrBlock="0.0.0.0/0",
            NatGatewayId=nat_id,
        )
        for subnet_id in private_subnets:
            self.ec2.associate_route_table(SubnetId=subnet_id, RouteTableId=private_rt)

        self.logger.info(
            f"✓ VPC ready: {vpc_id} "
            f"(public={public_subnets}, private={private_subnets})"
        )
        return {
            "vpc_id": vpc_id,
            "public_subnets": public_subnets,
            "private_subnets": private_subnets,
        }

    def _ensure_nat_routing(
        self,
        vpc_id: str,
        public_subnets: List[str],
        private_subnets: List[str],
    ) -> None:
        """Ensure private subnets have 0.0.0.0/0 → NAT (idempotent for reused VPCs)."""
        nats = self.ec2.describe_nat_gateways(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "state", "Values": ["available", "pending"]},
            ]
        ).get("NatGateways") or []
        nat_id = None
        for nat in nats:
            tags = {t["Key"]: t["Value"] for t in (nat.get("Tags") or [])}
            if tags.get("Name") == f"nat-for-{self.project_name}":
                nat_id = nat["NatGatewayId"]
                if nat["State"] == "pending":
                    self.ec2.get_waiter("nat_gateway_available").wait(
                        NatGatewayIds=[nat_id]
                    )
                break
        if not nat_id and nats:
            nat_id = nats[0]["NatGatewayId"]
        if not nat_id:
            eip = self.ec2.allocate_address(Domain="vpc")["AllocationId"]
            nat_id = self.ec2.create_nat_gateway(
                SubnetId=public_subnets[0],
                AllocationId=eip,
                TagSpecifications=[
                    {
                        "ResourceType": "natgateway",
                        "Tags": [
                            {
                                "Key": "Name",
                                "Value": f"nat-for-{self.project_name}",
                            }
                        ],
                    }
                ],
            )["NatGateway"]["NatGatewayId"]
            self.logger.info(f"  Waiting for NAT Gateway: {nat_id}")
            self.ec2.get_waiter("nat_gateway_available").wait(NatGatewayIds=[nat_id])

        private_rt_name = f"private-rt-for-{self.project_name}"
        route_table_id = None
        rts = self.ec2.describe_route_tables(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        ).get("RouteTables") or []
        for rt in rts:
            tags = {t["Key"]: t["Value"] for t in (rt.get("Tags") or [])}
            if tags.get("Name") == private_rt_name:
                route_table_id = rt["RouteTableId"]
                break
            for route in rt.get("Routes") or []:
                if route.get("NatGatewayId") == nat_id:
                    route_table_id = rt["RouteTableId"]
                    break
            if route_table_id:
                break

        if not route_table_id:
            route_table_id = self.ec2.create_route_table(
                VpcId=vpc_id,
                TagSpecifications=[
                    {
                        "ResourceType": "route-table",
                        "Tags": [{"Key": "Name", "Value": private_rt_name}],
                    }
                ],
            )["RouteTable"]["RouteTableId"]
            self.ec2.create_route(
                RouteTableId=route_table_id,
                DestinationCidrBlock="0.0.0.0/0",
                NatGatewayId=nat_id,
            )
        else:
            has_nat_route = False
            for rt in rts:
                if rt["RouteTableId"] != route_table_id:
                    continue
                for route in rt.get("Routes") or []:
                    if (
                        route.get("DestinationCidrBlock") == "0.0.0.0/0"
                        and route.get("NatGatewayId") == nat_id
                    ):
                        has_nat_route = True
            if not has_nat_route:
                try:
                    self.ec2.create_route(
                        RouteTableId=route_table_id,
                        DestinationCidrBlock="0.0.0.0/0",
                        NatGatewayId=nat_id,
                    )
                except ClientError as e:
                    if e.response["Error"]["Code"] == "RouteAlreadyExists":
                        self.ec2.replace_route(
                            RouteTableId=route_table_id,
                            DestinationCidrBlock="0.0.0.0/0",
                            NatGatewayId=nat_id,
                        )
                    else:
                        raise

        for subnet_id in private_subnets:
            associated = self.ec2.describe_route_tables(
                Filters=[{"Name": "association.subnet-id", "Values": [subnet_id]}]
            ).get("RouteTables") or []
            already = any(rt["RouteTableId"] == route_table_id for rt in associated)
            if already:
                continue
            for rt in associated:
                for assoc in rt.get("Associations") or []:
                    if assoc.get("SubnetId") == subnet_id and not assoc.get("Main"):
                        try:
                            self.ec2.disassociate_route_table(
                                AssociationId=assoc["RouteTableAssociationId"]
                            )
                        except ClientError:
                            pass
            try:
                self.ec2.associate_route_table(
                    RouteTableId=route_table_id, SubnetId=subnet_id
                )
            except ClientError as e:
                if e.response["Error"]["Code"] != "Resource.AlreadyAssociated":
                    self.logger.warning(
                        f"  Could not associate {subnet_id} with private RT: {e}"
                    )

    # --- VPC endpoints -------------------------------------------------------

    def ensure_private_subnet_vpc_endpoints(
        self, vpc_id: str, private_subnets: List[str]
    ) -> Dict[str, Optional[str]]:
        """
        Interface: ECR, Logs, Secrets Manager, Bedrock Runtime / AgentCore.
        Gateway: S3 + DynamoDB (Lambda jobs table / artifact traffic).
        """
        if not private_subnets:
            self.logger.warning("  Skipping VPC endpoints: no private subnets")
            return {}
        self.logger.info(
            "  Ensuring VPC endpoints (S3, DynamoDB, ECR, Logs, Secrets Manager, "
            "Bedrock Runtime, AgentCore)"
        )
        vpce_sg_id = self._ensure_vpce_security_group(vpc_id)
        endpoint_ids: Dict[str, Optional[str]] = {}
        interface_services = [
            (f"com.amazonaws.{self.region}.ecr.api", f"ecr-api-endpoint-{self.project_name}"),
            (f"com.amazonaws.{self.region}.ecr.dkr", f"ecr-dkr-endpoint-{self.project_name}"),
            (f"com.amazonaws.{self.region}.logs", f"logs-endpoint-{self.project_name}"),
            (
                f"com.amazonaws.{self.region}.secretsmanager",
                f"secretsmanager-endpoint-{self.project_name}",
            ),
            (
                f"com.amazonaws.{self.region}.bedrock-runtime",
                f"bedrock-endpoint-{self.project_name}",
            ),
            (
                f"com.amazonaws.{self.region}.bedrock-agentcore",
                f"bedrock-agentcore-endpoint-{self.project_name}",
            ),
            (
                f"com.amazonaws.{self.region}.bedrock-agentcore-control",
                f"bedrock-agentcore-control-endpoint-{self.project_name}",
            ),
        ]
        for service_name, endpoint_name in interface_services:
            endpoint_ids[service_name] = self._create_interface_vpc_endpoint(
                vpc_id=vpc_id,
                service_name=service_name,
                subnet_ids=private_subnets,
                security_group_ids=[vpce_sg_id],
                endpoint_name=endpoint_name,
            )

        route_table_ids = self._get_route_table_ids_for_subnets(private_subnets, vpc_id)
        # Also associate public RT so any public-subnet traffic can use gateways.
        public_subnets, _ = self._classify_subnets(vpc_id)
        if public_subnets:
            route_table_ids = list(
                set(route_table_ids)
                | set(self._get_route_table_ids_for_subnets(public_subnets, vpc_id))
            )
        endpoint_ids["s3"] = self._create_gateway_vpc_endpoint(
            vpc_id, route_table_ids, "s3", f"s3-endpoint-{self.project_name}"
        )
        endpoint_ids["dynamodb"] = self._create_gateway_vpc_endpoint(
            vpc_id,
            route_table_ids,
            "dynamodb",
            f"dynamodb-endpoint-{self.project_name}",
        )
        return endpoint_ids

    def _ensure_vpce_security_group(self, vpc_id: str) -> str:
        cidr = self.ec2.describe_vpcs(VpcIds=[vpc_id])["Vpcs"][0]["CidrBlock"]
        return self.create_security_group(
            vpc_id=vpc_id,
            group_name=self.vpce_sg_name(),
            description=f"Allow HTTPS to VPC endpoints for {self.project_name}",
            ingress_rules=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 443,
                    "ToPort": 443,
                    "IpRanges": [
                        {"CidrIp": cidr, "Description": "VPC HTTPS to endpoints"}
                    ],
                }
            ],
        )

    def create_security_group(
        self,
        vpc_id: str,
        group_name: str,
        description: str,
        ingress_rules: Optional[List[Dict]] = None,
    ) -> str:
        try:
            sg_id = self.ec2.create_security_group(
                GroupName=group_name,
                Description=description,
                VpcId=vpc_id,
                TagSpecifications=[
                    {
                        "ResourceType": "security-group",
                        "Tags": [
                            {"Key": "Name", "Value": group_name},
                            {"Key": "Project", "Value": self.project_name},
                        ],
                    }
                ],
            )["GroupId"]
            if ingress_rules:
                try:
                    self.ec2.authorize_security_group_ingress(
                        GroupId=sg_id, IpPermissions=ingress_rules
                    )
                except ClientError as e:
                    if e.response["Error"]["Code"] != "InvalidPermission.Duplicate":
                        self.logger.warning(f"  Could not add SG ingress: {e}")
            self.logger.info(f"  ✓ Security group created: {sg_id} ({group_name})")
            return sg_id
        except ClientError as e:
            if e.response["Error"]["Code"] != "InvalidGroup.Duplicate":
                raise
            sgs = self.ec2.describe_security_groups(
                Filters=[
                    {"Name": "group-name", "Values": [group_name]},
                    {"Name": "vpc-id", "Values": [vpc_id]},
                ]
            )
            sg_id = sgs["SecurityGroups"][0]["GroupId"]
            self.logger.info(f"  Reusing security group: {sg_id} ({group_name})")
            return sg_id

    def _get_route_table_ids_for_subnets(
        self, subnet_ids: List[str], vpc_id: str
    ) -> List[str]:
        route_table_ids = set()
        for subnet_id in subnet_ids:
            try:
                response = self.ec2.describe_route_tables(
                    Filters=[
                        {"Name": "association.subnet-id", "Values": [subnet_id]}
                    ]
                )
                if response["RouteTables"]:
                    route_table_ids.add(response["RouteTables"][0]["RouteTableId"])
            except Exception as e:
                self.logger.debug(f"Could not get route table for {subnet_id}: {e}")
        if not route_table_ids:
            response = self.ec2.describe_route_tables(
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "association.main", "Values": ["true"]},
                ]
            )
            if response["RouteTables"]:
                route_table_ids.add(response["RouteTables"][0]["RouteTableId"])
        return list(route_table_ids)

    def _create_interface_vpc_endpoint(
        self,
        vpc_id: str,
        service_name: str,
        subnet_ids: List[str],
        security_group_ids: List[str],
        endpoint_name: str,
    ) -> Optional[str]:
        existing = self.ec2.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": [service_name]},
            ]
        ).get("VpcEndpoints") or []
        if existing:
            endpoint_id = existing[0]["VpcEndpointId"]
            current = {g["GroupId"] for g in existing[0].get("Groups") or []}
            missing = [sg for sg in security_group_ids if sg not in current]
            if missing:
                try:
                    self.ec2.modify_vpc_endpoint(
                        VpcEndpointId=endpoint_id, AddSecurityGroupIds=missing
                    )
                except ClientError as e:
                    self.logger.debug(f"  VPCE SG update {service_name}: {e}")
            self.logger.debug(f"  Reusing VPC endpoint {service_name}: {endpoint_id}")
            return endpoint_id
        try:
            resp = self.ec2.create_vpc_endpoint(
                VpcId=vpc_id,
                ServiceName=service_name,
                VpcEndpointType="Interface",
                SubnetIds=subnet_ids,
                SecurityGroupIds=security_group_ids,
                PrivateDnsEnabled=True,
                TagSpecifications=[
                    {
                        "ResourceType": "vpc-endpoint",
                        "Tags": [
                            {"Key": "Name", "Value": endpoint_name},
                            {"Key": "Project", "Value": self.project_name},
                        ],
                    }
                ],
            )
            endpoint_id = resp["VpcEndpoint"]["VpcEndpointId"]
            self.logger.info(f"  Created VPC endpoint {service_name}: {endpoint_id}")
            return endpoint_id
        except ClientError as e:
            self.logger.warning(f"  Failed to create VPC endpoint {service_name}: {e}")
            return None

    def _create_gateway_vpc_endpoint(
        self,
        vpc_id: str,
        route_table_ids: List[str],
        service_suffix: str,
        endpoint_name: str,
    ) -> Optional[str]:
        service_name = f"com.amazonaws.{self.region}.{service_suffix}"
        if not route_table_ids:
            self.logger.warning(
                f"  Skipping {service_suffix} gateway endpoint: no route tables"
            )
            return None
        existing = self.ec2.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": [service_name]},
            ]
        ).get("VpcEndpoints") or []
        if existing:
            endpoint_id = existing[0]["VpcEndpointId"]
            current = set(existing[0].get("RouteTableIds") or [])
            missing = [rt for rt in route_table_ids if rt not in current]
            if missing:
                try:
                    self.ec2.modify_vpc_endpoint(
                        VpcEndpointId=endpoint_id, AddRouteTableIds=missing
                    )
                except ClientError as e:
                    self.logger.debug(f"  Gateway RT update {service_suffix}: {e}")
            self.logger.debug(
                f"  Reusing {service_suffix} gateway endpoint: {endpoint_id}"
            )
            return endpoint_id
        try:
            resp = self.ec2.create_vpc_endpoint(
                VpcId=vpc_id,
                ServiceName=service_name,
                VpcEndpointType="Gateway",
                RouteTableIds=route_table_ids,
                TagSpecifications=[
                    {
                        "ResourceType": "vpc-endpoint",
                        "Tags": [
                            {"Key": "Name", "Value": endpoint_name},
                            {"Key": "Project", "Value": self.project_name},
                        ],
                    }
                ],
            )
            endpoint_id = resp["VpcEndpoint"]["VpcEndpointId"]
            self.logger.info(
                f"  Created {service_suffix} gateway endpoint: {endpoint_id}"
            )
            return endpoint_id
        except ClientError as e:
            self.logger.warning(
                f"  Failed to create {service_suffix} gateway endpoint: {e}"
            )
            return None
