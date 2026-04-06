package message_handler

import (
	"context"
	"fmt"
	"slices"

	ciliumv2 "github.com/cilium/cilium/pkg/k8s/apis/cilium.io/v2"
	ciliumclientset "github.com/cilium/cilium/pkg/k8s/client/clientset/versioned"
	v1 "github.com/cilium/cilium/pkg/k8s/slim/k8s/apis/meta/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

type CiliumBgpAdvert struct {
	matchExpressions []matchExpression
	advertTypes      []string
	app              AnApp
	manifest         *ciliumv2.CiliumBGPAdvertisement
	// change advertTypes to iota enum eventually
}

func (c1 CiliumBgpAdvert) Equal(c2 CiliumBgpAdvert) bool {
	return slices.Equal(c1.advertTypes, c2.advertTypes) && slices.Equal(c1.matchExpressions, c2.matchExpressions)
}

func (c CiliumBgpAdvert) matchExpressionsAsMap() map[string]string {
	labels := map[string]string{}
	for _, m := range c.matchExpressions {
		for k, v := range m.asMap() {
			labels[k] = v
		}
	}
	return labels

}
func (c *CiliumBgpAdvert) getListOptLabelSelector() string {
	selectors := labels.Set{}
	for _, m := range c.matchExpressions {
		for k, v := range m.asMap() {
			selectors[k] = v
		}
	}

	return selectors.AsSelector().String()
}

func (c *CiliumBgpAdvert) getLabelSelector() *v1.LabelSelector {
	selectors := map[string]string{}
	for _, m := range c.matchExpressions {
		for k, v := range m.asMap() {
			selectors[k] = v
		}
	}
	return &v1.LabelSelector{
		MatchLabels: selectors,
	}
}

func (c CiliumBgpAdvert) getBgpSvcAddrType() []ciliumv2.BGPServiceAddressType {
	b := []ciliumv2.BGPServiceAddressType{}
	for _, advType := range c.advertTypes {
		switch advType {
		case "loadbalancer":
			b = append(b, ciliumv2.BGPLoadBalancerIPAddr)
		case "external_ip":
			b = append(b, ciliumv2.BGPExternalIPAddr)
		case "cluster_ip":
			b = append(b, ciliumv2.BGPClusterIPAddr)
		default:
			continue
		}
	}
	return b
}

func (c *CiliumBgpAdvert) newManifest() *ciliumv2.CiliumBGPAdvertisement {
	return &ciliumv2.CiliumBGPAdvertisement{
		TypeMeta:   metav1.TypeMeta{Kind: "CiliumBGPAdvertisement", APIVersion: "cilium.io/v2"},
		ObjectMeta: metav1.ObjectMeta{GenerateName: c.app.Name, Labels: c.matchExpressionsAsMap()},
		Spec: ciliumv2.CiliumBGPAdvertisementSpec{
			Advertisements: []ciliumv2.BGPAdvertisement{
				{
					AdvertisementType: ciliumv2.BGPServiceAdvert,
					Service: &ciliumv2.BGPServiceOptions{
						Addresses:             c.getBgpSvcAddrType(),
						AggregationLengthIPv4: nil,
						AggregationLengthIPv6: nil,
					},
					Selector: c.getLabelSelector(),
				},
			},
		},
	}
}

func (c CiliumBgpAdvert) checkExists(clientset ciliumclientset.Interface, ctx context.Context) (bool, error) {
	policies, err := clientset.CiliumV2().CiliumBGPAdvertisements().List(ctx, metav1.ListOptions{
		LabelSelector: c.getListOptLabelSelector(),
	})
	if err != nil {
		return false, err
	}
	return len(policies.Items) > 0, nil
}

func (c *CiliumBgpAdvert) getActiveAdv(clientset ciliumclientset.Interface, ctx context.Context) (*ciliumv2.CiliumBGPAdvertisement, error) {
	policies, err := clientset.CiliumV2().CiliumBGPAdvertisements().List(ctx, metav1.ListOptions{
		LabelSelector: c.getListOptLabelSelector(),
	})
	if err != nil {
		return nil, err
	}
	cAdv := policies.Items[0]
	// double check if this could ever be more than 1 item... versioning etc
	return &cAdv, nil
}

func (c CiliumBgpAdvert) isRemovable(clientset kubernetes.Clientset, namespace string, ctx context.Context) (bool, error) {
	allDepls, err := clientset.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, depl := range allDepls.Items {
		a2 := NewApp(depl.Name, depl.Labels)
		if c.app.tagsEqual(a2) {
			return false, nil
		}
	}
	return true, nil
}

func (c *CiliumBgpAdvert) removeAdv(clientset ciliumclientset.Interface, ctx context.Context) error {
	a, err := c.getActiveAdv(clientset, ctx)
	if err != nil {
		return err
	}
	return clientset.CiliumV2().CiliumBGPAdvertisements().Delete(ctx, a.Name, metav1.DeleteOptions{})
}

func (c *CiliumBgpAdvert) applyManifest(clientset ciliumclientset.Interface, ctx context.Context) error {
	if exists, err := c.checkExists(clientset, ctx); exists && err == nil {
		if c.manifest == nil {
			manifest, err := c.getActiveAdv(clientset, ctx)
			if err != nil {
				return fmt.Errorf("applyManifest getActiveAdv %w", err)
			}
			if manifest != nil {
				c.manifest = manifest
			} else {
				// this shouldn't ever be with checkExists == true
				c.manifest = c.newManifest()
			}
		}
		updatedManifest, err := clientset.CiliumV2().CiliumBGPAdvertisements().Update(ctx, c.manifest, metav1.UpdateOptions{TypeMeta: c.manifest.TypeMeta})
		if err != nil {
			return fmt.Errorf("applyManifest Update %w", err)
		}
		c.manifest = updatedManifest
	} else if err == nil {
		if c.manifest == nil {
			c.manifest = c.newManifest()
		}
		updatedManifest, err := clientset.CiliumV2().CiliumBGPAdvertisements().Create(ctx, c.manifest, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("applyManifest Create %w", err)
		}
		c.manifest = updatedManifest
	} else {
		return fmt.Errorf("applyManifest checkExists %w", err)
	}
	return nil
}

func NewCiliumBGPAdvert(a AnApp) CiliumBgpAdvert {
	return CiliumBgpAdvert{
		matchExpressions: []matchExpression{
			{key: "vrf", value: a.getVrf()},
			{key: "tenantName", value: a.getTenantName()},
		},
		advertTypes: a.getAdvertiseTypes(),
		app:         a,
		manifest:    nil,
	}
}
