#!/bin/python
#coding=UTF_8
"""

    作者：张江涛
    功能：S3 对象存储列出桶中的对象
    日期：2020/10/15
    环境信息：Python版本：2.7  Boto3版本： 1.14.48

"""
import boto3
from botocore.client import Config
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
class S3BOTO3DEMO():
    def __init__(self):
        # AK 信息
        access_key = 'H7LAU3AO9AIF9XISCDS6'
        # SK信息
        secret_key = '8pyPnHnHn2DJFrAlKCggOO8SxJLbL68O7o98l6o1'
        # Endpoint信息
        self.url = 'http://10.255.20.145:8060'
        # 桶名称
        self.bucket_name = 'bucket-zjt'
        # 连接S3
        try:
            self.s3_client = boto3.client("s3", endpoint_url=self.url,
                                          aws_access_key_id=access_key,
                                          aws_secret_access_key=secret_key,
                                          config=Config(signature_version='s3v4', connect_timeout=3000,
                                                       read_timeout=3000)
                                          )
        except Exception as e:
            print(e)
    def list_object(self):
        #list对象
        contoken=''
        try:
            while True:

                response=self.s3_client.list_objects_v2(
                                                    Bucket=self.bucket_name,
                                                    MaxKeys=3,
                                                    ContinuationToken=contoken
                                                    )
                if response.has_key('Contents'):
                    count=len(response['Contents'])
                    for i in range(0,count):
                        print("对象名:"+response['Contents'][i]['Key']+"---对象大小:"+str(response['Contents'][i]['Size']))
                    if response['IsTruncated']==True:
                        contoken=response['NextContinuationToken']
                    if response['IsTruncated']==False:
                        break
                else:
                    print("没有文件")
                    break
        except Exception as e:
            print(e)
if __name__ == '__main__':
    s3_boto3 = S3BOTO3DEMO()
    s3_boto3.list_object()