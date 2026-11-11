package modelarts

import (
	"context"
	"time"

	"github.com/hashicorp/go-uuid"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/retry"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	"github.com/chnsz/golangsdk"

	"github.com/huaweicloud/terraform-provider-huaweicloud/huaweicloud/config"
	"github.com/huaweicloud/terraform-provider-huaweicloud/huaweicloud/services/cbc"
	"github.com/huaweicloud/terraform-provider-huaweicloud/huaweicloud/utils"
)

var v2NodeBatchUnsubscribeNonUpdatableParams = []string{
	"resource_pool_name",
	"node_ids",
	"resource_ids",
}

// @API BSS POST /v2/orders/suscriptions/resources/query
// @API BSS POST /v2/orders/subscriptions/resources/unsubscribe
// @API ModelArts GET /v2/{project_id}/pools/{pool_name}/nodes
func ResourceV2NodeBatchUnsubscribe() *schema.Resource {
	return &schema.Resource{
		CreateContext: resourceV2NodeBatchUnsubscribeCreate,
		ReadContext:   resourceV2NodeBatchUnsubscribeRead,
		UpdateContext: resourceV2NodeBatchUnsubscribeUpdate,
		DeleteContext: resourceV2NodeBatchUnsubscribeDelete,

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(45 * time.Minute),
		},

		CustomizeDiff: config.FlexibleForceNew(v2NodeBatchUnsubscribeNonUpdatableParams),

		Schema: map[string]*schema.Schema{
			"region": {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				ForceNew:    true,
				Description: `The region where the resource nodes are located.`,
			},
			"resource_pool_name": {
				Type:        schema.TypeString,
				Required:    true,
				Description: `The resource pool name to which the resource nodes belong.`,
			},
			"node_ids": {
				Type:        schema.TypeList,
				Required:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: `The ID list of resource nodes to be unsubscribed.`,
			},
			"resource_ids": {
				Type:     schema.TypeList,
				Optional: true,
				Elem:     &schema.Schema{Type: schema.TypeString},
				Deprecated: `This parameter is deprecated, the resource IDs to unsubscribe are automatically resolved
from the nodes, and no longer needs to be specified.`,
				Description: `The ID list of resources to be deleted.`,
			},
			"enable_force_new": {
				Type:         schema.TypeString,
				Optional:     true,
				ValidateFunc: validation.StringInSlice([]string{"true", "false"}, false),
				Description:  utils.SchemaDesc("", utils.SchemaDescInput{Internal: true}),
			},
		},
	}
}

func waitForV2NodeBatchUnsubscribeCompleted(ctx context.Context, client *golangsdk.ServiceClient, resourcePoolName string,
	unsubscribeNodeIds []interface{}, timeout time.Duration) error {
	stateConf := &retry.StateChangeConf{
		Pending: []string{"PENDING"},
		Target:  []string{"COMPLETED"},
		Refresh: func() (interface{}, string, error) {
			nodes, err := listV2ResourcePoolNodes(client, resourcePoolName)
			// 404: The resource pool does not exist.
			if _, ok := err.(golangsdk.ErrDefault404); ok {
				return "deleted", "COMPLETED", nil
			}
			if err != nil {
				return nil, "ERROR", err
			}

			resourceIds := utils.PathSearch(`[*].metadata.labels."os.modelarts/resource.id"`, nodes,
				make([]interface{}, 0)).([]interface{})
			if utils.IsSliceContainsAnyAnotherSliceElement(utils.ExpandToStringList(resourceIds),
				utils.ExpandToStringList(unsubscribeNodeIds), false, true) {
				return resourceIds, "PENDING", nil
			}
			return resourceIds, "COMPLETED", nil
		},
		Timeout:      timeout,
		Delay:        30 * time.Second,
		PollInterval: 30 * time.Second,
	}
	_, err := stateConf.WaitForStateContext(ctx)
	return err
}

// resolveV2NodeUnsubscribeResourceIds resolves the BSS resource IDs used to unsubscribe the specified nodes.
// The A5 hyper nodes are subscribed in batches, the batch UID is used as the unsubscribe resource ID, and the
// duplicate batch UIDs are removed. For other nodes, the node IDs themselves are used.
// The node IDs that do not match any node are kept as-is to keep the unsubscribe behavior unchanged.
func resolveV2NodeUnsubscribeResourceIds(nodes []interface{}, nodeIds []interface{}) []interface{} {
	// The key is the node ID (the value of the 'os.modelarts/resource.id' label), and the value is the
	// corresponding batch UID (the value of the 'os.modelarts.node/batch.uid' label).
	nodeBatchUids := make(map[string]string)
	for _, node := range nodes {
		labels := utils.PathSearch(`metadata.labels`, node, nil)
		resourceId := utils.PathSearch(`"os.modelarts/resource.id"`, labels, "").(string)
		if resourceId == "" {
			continue
		}
		nodeBatchUids[resourceId] = utils.PathSearch(`"os.modelarts.node/batch.uid"`, labels, "").(string)
	}

	resourceIdSet := make(map[string]struct{})
	result := make([]interface{}, 0, len(nodeIds))
	for _, nodeId := range utils.ExpandToStringList(nodeIds) {
		// For the A5 hyper nodes, unsubscribe the batch to which the node belongs.
		if batchUid := nodeBatchUids[nodeId]; batchUid != "" {
			nodeId = batchUid
		}
		if _, ok := resourceIdSet[nodeId]; ok {
			continue
		}
		resourceIdSet[nodeId] = struct{}{}
		result = append(result, nodeId)
	}
	return result
}

func resourceV2NodeBatchUnsubscribeCreate(ctx context.Context, d *schema.ResourceData, meta interface{}) diag.Diagnostics {
	var (
		cfg               = meta.(*config.Config)
		region            = cfg.GetRegion(d)
		resourcePoolName  = d.Get("resource_pool_name").(string)
		deleteNodeIds     = d.Get("node_ids").([]interface{})
		deleteResourceIds = d.Get("resource_ids").([]interface{})
	)

	bssClient, err := cfg.NewServiceClient("bssv2", cfg.GetRegion(d))
	if err != nil {
		return diag.Errorf("error creating BSS client: %s", err)
	}

	modelartsClient, err := cfg.NewServiceClient("modelarts", region)
	if err != nil {
		return diag.Errorf("error creating ModelArts client: %s", err)
	}

	// The parameter 'resource_ids' is deprecated, and the resource IDs to unsubscribe are automatically
	// resolved from the nodes under the specified resource pool.
	unsubscribeResourceIds := deleteResourceIds
	if len(unsubscribeResourceIds) == 0 {
		nodes, err := listV2ResourcePoolNodes(modelartsClient, resourcePoolName)
		if err != nil {
			if _, ok := err.(golangsdk.ErrDefault404); !ok {
				return diag.Errorf("error querying nodes under specified resource pool (%s): %s", resourcePoolName, err)
			}
			// If the resource pool does not exist, it is considered that all nodes have been deleted, and the
			// node IDs are used as the resource IDs to keep the unsubscribe behavior unchanged.
			nodes = make([]interface{}, 0)
		}
		unsubscribeResourceIds = resolveV2NodeUnsubscribeResourceIds(nodes, deleteNodeIds)
	}

	// Delete
	err = cbc.UnsubscribePrePaidResources(bssClient, unsubscribeResourceIds)
	if err != nil {
		return diag.FromErr(err)
	}
	err = cbc.WaitForResourcesUnsubscribed(ctx, bssClient, unsubscribeResourceIds, d.Timeout(schema.TimeoutCreate))
	if err != nil {
		return diag.Errorf("error waiting for all resources to be unsubscribed: %s ", err)
	}

	randomUUID, err := uuid.GenerateUUID()
	if err != nil {
		return diag.Errorf("unable to generate ID: %s", err)
	}
	d.SetId(randomUUID)

	err = waitForV2NodeBatchUnsubscribeCompleted(ctx, modelartsClient, resourcePoolName, deleteNodeIds,
		d.Timeout(schema.TimeoutCreate))
	if err != nil {
		return diag.FromErr(err)
	}

	return resourceV2NodeBatchUnsubscribeRead(ctx, d, meta)
}

func resourceV2NodeBatchUnsubscribeRead(_ context.Context, _ *schema.ResourceData, _ interface{}) diag.Diagnostics {
	return nil
}

func resourceV2NodeBatchUnsubscribeUpdate(_ context.Context, _ *schema.ResourceData, _ interface{}) diag.Diagnostics {
	return nil
}

func resourceV2NodeBatchUnsubscribeDelete(_ context.Context, _ *schema.ResourceData, _ interface{}) diag.Diagnostics {
	errorMsg := `This resource is only a one-time action resource for batch delete the ModelArts nodes. Deleting this
resource will not clear the corresponding request record, but will only remove the resource information from the tfstate
file.`
	return diag.Diagnostics{
		diag.Diagnostic{
			Severity: diag.Warning,
			Summary:  errorMsg,
		},
	}
}
