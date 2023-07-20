from typing import TypeVar

from django.db import models

from ...core.models import SortableModel
from ...product.models import Product, ProductType
from .base import AssociatedAttributeManager, Attribute, BaseAttributeQuerySet

T = TypeVar("T", bound=models.Model)


class AssignedProductAttributeValueQuerySet(BaseAttributeQuerySet[T]):
    def get_values_for_attribute(self, attribute: Attribute):
        return self.filter(value__attribute_id=attribute.pk)


AssignedProductAttributeValueManager = models.Manager.from_queryset(
    AssignedProductAttributeValueQuerySet
)


class AssignedProductAttributeValue(SortableModel):
    value = models.ForeignKey(
        "AttributeValue",
        on_delete=models.CASCADE,
        related_name="productvalueassignment",
    )
    product = models.ForeignKey(
        Product,
        related_name="attributevalues",
        on_delete=models.CASCADE,
    )

    objects = AssignedProductAttributeValueManager()

    class Meta:
        unique_together = (("value", "product"),)
        ordering = ("sort_order", "pk")

    def get_ordering_queryset(self):
        return self.product.attributevalues.all()


class AttributeProduct(SortableModel):
    attribute = models.ForeignKey(
        "Attribute", related_name="attributeproduct", on_delete=models.CASCADE
    )
    product_type = models.ForeignKey(
        ProductType, related_name="attributeproduct", on_delete=models.CASCADE
    )

    objects = AssociatedAttributeManager()

    class Meta:
        unique_together = (("attribute", "product_type"),)
        ordering = ("sort_order", "pk")

    def get_ordering_queryset(self):
        return self.product_type.attributeproduct.all()
