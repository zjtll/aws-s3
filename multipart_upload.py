#!/bin/python
#coding=UTF_8
"""

    作者：张江涛
    功能：S3 对象存储分段上传
    日期：2020/10/14
    环境信息：Python版本：2.7  Boto3版本： 1.14.48

"""

import boto3
import json
import os
import math
import hashlib
from boto3.session import Session
from botocore.client import Config
from botocore.exceptions import ClientError

class S3BOTO3DEMO():

    def __init__(self):

        # AK 信息
        access_key = 'H7LAU3AO9AIF9XISCDS6'
        # SK信息
        secret_key = '8pyPnHnHn2DJFrAlKCggOO8SxJLbL68O7o98l6o1'
        # 桶名称
        self.bucket_name = 'bucket-zjt'
        # Endpoint信息
        self.url = 'http://10.255.20.145:8060'
        # 待上传对象名称
        self.file_name = 'hdm.zip'
        #连接S3的两种方式
        try:
            self.s3_client = boto3.client("s3", endpoint_url=self.url,
                                          aws_access_key_id=access_key,
                                          aws_secret_access_key=secret_key,
                                          config=Config(signature_version='s3v4', connect_timeout=3000,
                                                        read_timeout=3000)
                                          )
            # self.session = Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            # # 连接S3对象存储
            # self.s3_client = self.session.client('s3', endpoint_url=self.url,
            #
            #                                      # signature_version 默认为，v2版本，指定v4版本
            #                                      # connect_timeout 超时参数，单位 s, 默认是60s, 如下是5分钟
            #                                      # read_timeout 读超时参数，单位 s，默认是60s, 如下是5分钟
            #                                      config=Config(#signature_version='s3v4',
            #                                            connect_timeout = 3000,read_timeout = 3000)
            #
            #                                     )

        except Exception as e:
            print(e)

    def multipart_upload(self):
        #分段上传
        try:
            #分段上传，选取文件为'hdm.zip',
            filetest=self.file_name
            # 设置的分段大小为7M
            chunksize=7*1024*1024

            # 创建分段上传获取uploadid
            response = self.s3_client.create_multipart_upload(
                ACL='public-read',
                Bucket=self.bucket_name,
                Key=filetest
            )
            uploadid = response['UploadId']
            self.upid = response['UploadId']
           # print(uploadid)
            with open(filetest,'rb') as test:
                info = []
                md5info=[]
                i=0
                while True:
                    op=test.read(chunksize)
                    if  not op:
                        break
                    md5 = hashlib.md5(op).hexdigest()
                    md5info.append(md5)
                    i+=1
                    # 开始分段上传
                    upload_part_response = self.s3_client.upload_part(
                        Body=op,
                        Bucket=self.bucket_name,
                        Key=filetest,
                        PartNumber=i,
                        UploadId=uploadid
                    )
                    etag = upload_part_response['ETag']
                    tmp = {'ETag': etag, 'PartNumber': i}
                    info.append(tmp)
                    print(upload_part_response)
                print(md5info)
                MultipartUpload = {'Parts': info}
                print(json.dumps(MultipartUpload, sort_keys=True, indent=2))

            #完成分段上传
            compute_multipart_upload_response = self.s3_client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=filetest,
                MultipartUpload=MultipartUpload,
                UploadId=uploadid
            )
        except Exception as e:
            print(e)

        #如果上传失败清理分段残留
        try:
            if compute_multipart_upload_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print("上传成功")
        except Exception as e:
            print(e)
            print("上传失败开始清理分片...")
            s3_boto3.abort_multipart_upload()

    def abort_multipart_upload(self):
        # 清除分段上传残留
        try:
            while True:
                abort_response = self.s3_client.abort_multipart_upload(
                    Bucket=self.bucket_name,
                    Key='hdm.zip',
                    UploadId=self.upid
                )
                print(abort_response)
                response = self.s3_client.list_parts(
                    Bucket=self.bucket_name,
                    Key='hdm.zip',
                    UploadId=self.upid
                )
                print(response)
                if response['IsTruncated'] == False:
                    break
                else:
                    continue
        except Exception as e:
            pass
#__name__是指当前py文件调用方式的方法，如果它等于main就直接执行，不能被其他文件导入使用
if __name__ == "__main__":
    s3_boto3 = S3BOTO3DEMO()
    s3_boto3.multipart_upload()




