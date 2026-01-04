import { EC2Client, DescribeInstancesCommand } from "@aws-sdk/client-ec2";
import express from 'express';
import http from 'http';

const REGION = process.env.AWS_REGION || "us-east-1";
const TAG_KEY = "Cluster";

const app = express();
const port = 8000;
const hostname = '0.0.0.0';

const ec2 = new EC2Client({ region: REGION });

let cluster1Instances = [];
let cluster2Instances = [];

let bestCluster1Instance = '';
let bestCluster2Instance = '';

async function listTargets(cluster) {
  let nextToken;
  const ips = [];

  do {
    const cmd = new DescribeInstancesCommand({
      NextToken: nextToken,
      Filters: [
        { Name: `tag:${TAG_KEY}`, Values: [cluster] },
        { Name: "instance-state-name", Values: ["running"] }
      ]
    });
    const res = await ec2.send(cmd);
    for (const r of res.Reservations ?? []) {
      for (const i of r.Instances ?? []) {
        if (i.PrivateIpAddress) ips.push(`http://${i.PrivateIpAddress}:8000/${cluster}`);
      }
    }
    nextToken = res.NextToken;
  } while (nextToken);

  return ips;
}

async function statusCheckRequest(url) {
    try {
        const res = await fetch(url, { method: 'GET', signal: AbortSignal.timeout(2000) });
        if (res.ok) {
            return;
        } else {
            throw new Error(`HTTP error! status: ${res.status}`);
        }
    } catch (error) {
        console.error(`Error fetching status from ${url}:`, error);
        throw error;
    }
}

async function statusCheckCluster(clusterInstances) {
    let bestInstance = '';
    let bestResponseTime = Infinity;

    for (const instance of clusterInstances) {
        try {
            const t0 = performance.now();
            await statusCheckRequest(instance);
            const responseTime = performance.now() - t0;

            if (responseTime < bestResponseTime) {
                bestResponseTime = responseTime;
                bestInstance = instance;
            }
        } catch (error) {
            console.error(`Error checking status for ${instance}:`, error);
        }
    }

    return bestInstance;
}

let checking = false;
async function statusCheckLoop() {
    if (checking) return;

    checking = true;
    bestCluster1Instance = await statusCheckCluster(cluster1Instances);
    bestCluster2Instance = await statusCheckCluster(cluster2Instances);

    setInterval(async () => {
        bestCluster1Instance = await statusCheckCluster(cluster1Instances);
        bestCluster2Instance = await statusCheckCluster(cluster2Instances);
    }, 100);
    checking = false;
}


statusCheckLoop();

function forwardRequest(req, res, target) {
    http.get(target, (response) => {
        let body = '';

        response.on('data', (chunk) => {
            body += chunk;
        });

        response.on('end', () => {
            res.status(200).json(JSON.parse(body));
        });
    }).on('error', (err) => {
        console.error(`Error forwarding request to ${target}:`, err);
        res.status(500).send('Internal Server Error');
    });
}

async function init() {
    cluster1Instances = await listTargets('cluster1');
    cluster2Instances = await listTargets('cluster2');
    console.log('Cluster 1 Instances:', cluster1Instances);
    console.log('Cluster 2 Instances:', cluster2Instances);
}

init();

app.get('/cluster1', (req, res, next) => {
    console.log(`Forwarding request to cluster1 instance: ${bestCluster1Instance}`);
    forwardRequest(req, res, `${bestCluster1Instance}`);
});

app.get('/cluster2', (req, res, next) => {
    console.log(`Forwarding request to cluster2 instance: ${bestCluster2Instance}`);
    forwardRequest(req, res, `${bestCluster2Instance}`);
});

app.listen(port, hostname, () => {
    console.log(`App listening on ${hostname}:${port}`);
});
