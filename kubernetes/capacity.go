package kubernetes

import "k8s.io/apimachinery/pkg/api/resource"

func parseCapacity(s string) (resource.Quantity, error) { return resource.ParseQuantity(s) }
