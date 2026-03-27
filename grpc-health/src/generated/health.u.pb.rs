const _: () = ::protobuf::__internal::assert_compatible_gencode_version(
    "4.34.0-release",
);
pub(crate) static mut grpc__health__v1__HealthCheckRequest_msg_init: ::protobuf::__internal::runtime::MiniTableInitPtr = ::protobuf::__internal::runtime::MiniTableInitPtr(
    ::protobuf::__internal::runtime::MiniTablePtr::dangling(),
);
#[allow(non_camel_case_types)]
pub struct HealthCheckRequest {
    inner: ::protobuf::__internal::runtime::OwnedMessageInner<HealthCheckRequest>,
}
impl ::protobuf::Message for HealthCheckRequest {}
impl ::std::default::Default for HealthCheckRequest {
    fn default() -> Self {
        Self::new()
    }
}
impl ::std::fmt::Debug for HealthCheckRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "{}", ::protobuf::__internal::runtime::debug_string(self))
    }
}
unsafe impl Sync for HealthCheckRequest {}
unsafe impl Send for HealthCheckRequest {}
impl ::protobuf::Proxied for HealthCheckRequest {
    type View<'msg> = HealthCheckRequestView<'msg>;
}
impl ::protobuf::__internal::SealedInternal for HealthCheckRequest {}
impl ::protobuf::MutProxied for HealthCheckRequest {
    type Mut<'msg> = HealthCheckRequestMut<'msg>;
}
#[derive(Copy, Clone)]
#[allow(dead_code)]
pub struct HealthCheckRequestView<'msg> {
    inner: ::protobuf::__internal::runtime::MessageViewInner<'msg, HealthCheckRequest>,
}
impl<'msg> ::protobuf::__internal::SealedInternal for HealthCheckRequestView<'msg> {}
impl<'msg> ::protobuf::MessageView<'msg> for HealthCheckRequestView<'msg> {
    type Message = HealthCheckRequest;
}
impl ::std::fmt::Debug for HealthCheckRequestView<'_> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "{}", ::protobuf::__internal::runtime::debug_string(self))
    }
}
impl ::std::default::Default for HealthCheckRequestView<'_> {
    fn default() -> HealthCheckRequestView<'static> {
        ::protobuf::__internal::runtime::MessageViewInner::default().into()
    }
}
impl<
    'msg,
> From<::protobuf::__internal::runtime::MessageViewInner<'msg, HealthCheckRequest>>
for HealthCheckRequestView<'msg> {
    fn from(
        inner: ::protobuf::__internal::runtime::MessageViewInner<
            'msg,
            HealthCheckRequest,
        >,
    ) -> Self {
        Self { inner }
    }
}
#[allow(dead_code)]
impl<'msg> HealthCheckRequestView<'msg> {
    pub fn to_owned(&self) -> HealthCheckRequest {
        ::protobuf::IntoProxied::into_proxied(*self, ::protobuf::__internal::Private)
    }
    pub fn service(self) -> ::protobuf::View<'msg, ::protobuf::ProtoString> {
        let str_view = unsafe { self.inner.ptr().get_string_at_index(0, (b"").into()) };
        unsafe { ::protobuf::ProtoStr::from_utf8_unchecked(str_view.as_ref()) }
    }
}
unsafe impl Sync for HealthCheckRequestView<'_> {}
unsafe impl Send for HealthCheckRequestView<'_> {}
impl<'msg> ::protobuf::AsView for HealthCheckRequestView<'msg> {
    type Proxied = HealthCheckRequest;
    fn as_view(&self) -> ::protobuf::View<'msg, HealthCheckRequest> {
        *self
    }
}
impl<'msg> ::protobuf::IntoView<'msg> for HealthCheckRequestView<'msg> {
    fn into_view<'shorter>(self) -> HealthCheckRequestView<'shorter>
    where
        'msg: 'shorter,
    {
        self
    }
}
impl<'msg> ::protobuf::IntoProxied<HealthCheckRequest> for HealthCheckRequestView<'msg> {
    fn into_proxied(
        self,
        _private: ::protobuf::__internal::Private,
    ) -> HealthCheckRequest {
        let mut dst = HealthCheckRequest::new();
        assert!(
            unsafe { dst.inner.ptr_mut().deep_copy(self.inner.ptr(), dst.inner.arena()) }
        );
        dst
    }
}
impl<'msg> ::protobuf::IntoProxied<HealthCheckRequest> for HealthCheckRequestMut<'msg> {
    fn into_proxied(
        self,
        _private: ::protobuf::__internal::Private,
    ) -> HealthCheckRequest {
        ::protobuf::IntoProxied::into_proxied(
            ::protobuf::IntoView::into_view(self),
            _private,
        )
    }
}
impl ::protobuf::__internal::runtime::EntityType for HealthCheckRequest {
    type Tag = ::protobuf::__internal::runtime::MessageTag;
}
impl<'msg> ::protobuf::__internal::runtime::EntityType for HealthCheckRequestView<'msg> {
    type Tag = ::protobuf::__internal::runtime::ViewProxyTag;
}
impl<'msg> ::protobuf::__internal::runtime::EntityType for HealthCheckRequestMut<'msg> {
    type Tag = ::protobuf::__internal::runtime::MutProxyTag;
}
#[allow(dead_code)]
#[allow(non_camel_case_types)]
pub struct HealthCheckRequestMut<'msg> {
    inner: ::protobuf::__internal::runtime::MessageMutInner<'msg, HealthCheckRequest>,
}
impl<'msg> ::protobuf::__internal::SealedInternal for HealthCheckRequestMut<'msg> {}
impl<'msg> ::protobuf::MessageMut<'msg> for HealthCheckRequestMut<'msg> {
    type Message = HealthCheckRequest;
}
impl ::std::fmt::Debug for HealthCheckRequestMut<'_> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "{}", ::protobuf::__internal::runtime::debug_string(self))
    }
}
impl<
    'msg,
> From<::protobuf::__internal::runtime::MessageMutInner<'msg, HealthCheckRequest>>
for HealthCheckRequestMut<'msg> {
    fn from(
        inner: ::protobuf::__internal::runtime::MessageMutInner<'msg, HealthCheckRequest>,
    ) -> Self {
        Self { inner }
    }
}
#[allow(dead_code)]
impl<'msg> HealthCheckRequestMut<'msg> {
    #[doc(hidden)]
    pub fn as_message_mut_inner(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessageMutInner<'msg, HealthCheckRequest> {
        self.inner
    }
    pub fn to_owned(&self) -> HealthCheckRequest {
        ::protobuf::AsView::as_view(self).to_owned()
    }
    pub fn service(&self) -> ::protobuf::View<'_, ::protobuf::ProtoString> {
        let str_view = unsafe { self.inner.ptr().get_string_at_index(0, (b"").into()) };
        unsafe { ::protobuf::ProtoStr::from_utf8_unchecked(str_view.as_ref()) }
    }
    pub fn set_service(
        &mut self,
        val: impl ::protobuf::IntoProxied<::protobuf::ProtoString>,
    ) {
        unsafe {
            ::protobuf::__internal::runtime::message_set_string_field(
                ::protobuf::AsMut::as_mut(self).inner,
                0,
                val,
            );
        }
    }
}
unsafe impl Send for HealthCheckRequestMut<'_> {}
unsafe impl Sync for HealthCheckRequestMut<'_> {}
impl<'msg> ::protobuf::AsView for HealthCheckRequestMut<'msg> {
    type Proxied = HealthCheckRequest;
    fn as_view(&self) -> ::protobuf::View<'_, HealthCheckRequest> {
        HealthCheckRequestView {
            inner: ::protobuf::__internal::runtime::MessageViewInner::view_of_mut(
                self.inner,
            ),
        }
    }
}
impl<'msg> ::protobuf::IntoView<'msg> for HealthCheckRequestMut<'msg> {
    fn into_view<'shorter>(self) -> ::protobuf::View<'shorter, HealthCheckRequest>
    where
        'msg: 'shorter,
    {
        HealthCheckRequestView {
            inner: ::protobuf::__internal::runtime::MessageViewInner::view_of_mut(
                self.inner,
            ),
        }
    }
}
impl<'msg> ::protobuf::AsMut for HealthCheckRequestMut<'msg> {
    type MutProxied = HealthCheckRequest;
    fn as_mut(&mut self) -> HealthCheckRequestMut<'msg> {
        HealthCheckRequestMut {
            inner: self.inner,
        }
    }
}
impl<'msg> ::protobuf::IntoMut<'msg> for HealthCheckRequestMut<'msg> {
    fn into_mut<'shorter>(self) -> HealthCheckRequestMut<'shorter>
    where
        'msg: 'shorter,
    {
        self
    }
}
#[allow(dead_code)]
impl HealthCheckRequest {
    pub fn new() -> Self {
        Self {
            inner: ::protobuf::__internal::runtime::OwnedMessageInner::<Self>::new(),
        }
    }
    #[doc(hidden)]
    pub fn as_message_mut_inner(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessageMutInner<'_, HealthCheckRequest> {
        ::protobuf::__internal::runtime::MessageMutInner::mut_of_owned(&mut self.inner)
    }
    pub fn as_view(&self) -> HealthCheckRequestView<'_> {
        ::protobuf::__internal::runtime::MessageViewInner::view_of_owned(&self.inner)
            .into()
    }
    pub fn as_mut(&mut self) -> HealthCheckRequestMut<'_> {
        ::protobuf::__internal::runtime::MessageMutInner::mut_of_owned(&mut self.inner)
            .into()
    }
    pub fn service(&self) -> ::protobuf::View<'_, ::protobuf::ProtoString> {
        let str_view = unsafe { self.inner.ptr().get_string_at_index(0, (b"").into()) };
        unsafe { ::protobuf::ProtoStr::from_utf8_unchecked(str_view.as_ref()) }
    }
    pub fn set_service(
        &mut self,
        val: impl ::protobuf::IntoProxied<::protobuf::ProtoString>,
    ) {
        unsafe {
            ::protobuf::__internal::runtime::message_set_string_field(
                ::protobuf::AsMut::as_mut(self).inner,
                0,
                val,
            );
        }
    }
}
impl ::std::ops::Drop for HealthCheckRequest {
    #[inline]
    fn drop(&mut self) {}
}
impl ::std::clone::Clone for HealthCheckRequest {
    fn clone(&self) -> Self {
        self.as_view().to_owned()
    }
}
impl ::protobuf::AsView for HealthCheckRequest {
    type Proxied = Self;
    fn as_view(&self) -> HealthCheckRequestView<'_> {
        self.as_view()
    }
}
impl ::protobuf::AsMut for HealthCheckRequest {
    type MutProxied = Self;
    fn as_mut(&mut self) -> HealthCheckRequestMut<'_> {
        self.as_mut()
    }
}
unsafe impl ::protobuf::__internal::runtime::AssociatedMiniTable for HealthCheckRequest {
    fn mini_table() -> ::protobuf::__internal::runtime::MiniTablePtr {
        static ONCE_LOCK: ::std::sync::OnceLock<
            ::protobuf::__internal::runtime::MiniTableInitPtr,
        > = ::std::sync::OnceLock::new();
        unsafe {
            ONCE_LOCK
                .get_or_init(|| {
                    super::grpc__health__v1__HealthCheckRequest_msg_init.0 = ::protobuf::__internal::runtime::build_mini_table(
                        "$M1P",
                    );
                    ::protobuf::__internal::runtime::link_mini_table(
                        super::grpc__health__v1__HealthCheckRequest_msg_init.0,
                        &[],
                        &[],
                    );
                    ::protobuf::__internal::runtime::MiniTableInitPtr(
                        super::grpc__health__v1__HealthCheckRequest_msg_init.0,
                    )
                })
                .0
        }
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetArena for HealthCheckRequest {
    fn get_arena(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> &::protobuf::__internal::runtime::Arena {
        self.inner.arena()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetMessagePtrMut for HealthCheckRequest {
    type Msg = HealthCheckRequest;
    fn get_ptr_mut(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessagePtr<HealthCheckRequest> {
        self.inner.ptr_mut()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetMessagePtr for HealthCheckRequest {
    type Msg = HealthCheckRequest;
    fn get_ptr(
        &self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessagePtr<HealthCheckRequest> {
        self.inner.ptr()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetMessagePtrMut
for HealthCheckRequestMut<'_> {
    type Msg = HealthCheckRequest;
    fn get_ptr_mut(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessagePtr<HealthCheckRequest> {
        self.inner.ptr_mut()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetMessagePtr
for HealthCheckRequestMut<'_> {
    type Msg = HealthCheckRequest;
    fn get_ptr(
        &self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessagePtr<HealthCheckRequest> {
        self.inner.ptr()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetMessagePtr
for HealthCheckRequestView<'_> {
    type Msg = HealthCheckRequest;
    fn get_ptr(
        &self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessagePtr<HealthCheckRequest> {
        self.inner.ptr()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetArena for HealthCheckRequestMut<'_> {
    fn get_arena(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> &::protobuf::__internal::runtime::Arena {
        self.inner.arena()
    }
}
pub(crate) static mut grpc__health__v1__HealthCheckResponse_msg_init: ::protobuf::__internal::runtime::MiniTableInitPtr = ::protobuf::__internal::runtime::MiniTableInitPtr(
    ::protobuf::__internal::runtime::MiniTablePtr::dangling(),
);
#[allow(non_camel_case_types)]
pub struct HealthCheckResponse {
    inner: ::protobuf::__internal::runtime::OwnedMessageInner<HealthCheckResponse>,
}
impl ::protobuf::Message for HealthCheckResponse {}
impl ::std::default::Default for HealthCheckResponse {
    fn default() -> Self {
        Self::new()
    }
}
impl ::std::fmt::Debug for HealthCheckResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "{}", ::protobuf::__internal::runtime::debug_string(self))
    }
}
unsafe impl Sync for HealthCheckResponse {}
unsafe impl Send for HealthCheckResponse {}
impl ::protobuf::Proxied for HealthCheckResponse {
    type View<'msg> = HealthCheckResponseView<'msg>;
}
impl ::protobuf::__internal::SealedInternal for HealthCheckResponse {}
impl ::protobuf::MutProxied for HealthCheckResponse {
    type Mut<'msg> = HealthCheckResponseMut<'msg>;
}
#[derive(Copy, Clone)]
#[allow(dead_code)]
pub struct HealthCheckResponseView<'msg> {
    inner: ::protobuf::__internal::runtime::MessageViewInner<'msg, HealthCheckResponse>,
}
impl<'msg> ::protobuf::__internal::SealedInternal for HealthCheckResponseView<'msg> {}
impl<'msg> ::protobuf::MessageView<'msg> for HealthCheckResponseView<'msg> {
    type Message = HealthCheckResponse;
}
impl ::std::fmt::Debug for HealthCheckResponseView<'_> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "{}", ::protobuf::__internal::runtime::debug_string(self))
    }
}
impl ::std::default::Default for HealthCheckResponseView<'_> {
    fn default() -> HealthCheckResponseView<'static> {
        ::protobuf::__internal::runtime::MessageViewInner::default().into()
    }
}
impl<
    'msg,
> From<::protobuf::__internal::runtime::MessageViewInner<'msg, HealthCheckResponse>>
for HealthCheckResponseView<'msg> {
    fn from(
        inner: ::protobuf::__internal::runtime::MessageViewInner<
            'msg,
            HealthCheckResponse,
        >,
    ) -> Self {
        Self { inner }
    }
}
#[allow(dead_code)]
impl<'msg> HealthCheckResponseView<'msg> {
    pub fn to_owned(&self) -> HealthCheckResponse {
        ::protobuf::IntoProxied::into_proxied(*self, ::protobuf::__internal::Private)
    }
    pub fn status(self) -> super::health_check_response::ServingStatus {
        unsafe {
            self.inner
                .ptr()
                .get_i32_at_index(
                    0,
                    (super::health_check_response::ServingStatus::Unknown).into(),
                )
                .try_into()
                .unwrap()
        }
    }
}
unsafe impl Sync for HealthCheckResponseView<'_> {}
unsafe impl Send for HealthCheckResponseView<'_> {}
impl<'msg> ::protobuf::AsView for HealthCheckResponseView<'msg> {
    type Proxied = HealthCheckResponse;
    fn as_view(&self) -> ::protobuf::View<'msg, HealthCheckResponse> {
        *self
    }
}
impl<'msg> ::protobuf::IntoView<'msg> for HealthCheckResponseView<'msg> {
    fn into_view<'shorter>(self) -> HealthCheckResponseView<'shorter>
    where
        'msg: 'shorter,
    {
        self
    }
}
impl<'msg> ::protobuf::IntoProxied<HealthCheckResponse>
for HealthCheckResponseView<'msg> {
    fn into_proxied(
        self,
        _private: ::protobuf::__internal::Private,
    ) -> HealthCheckResponse {
        let mut dst = HealthCheckResponse::new();
        assert!(
            unsafe { dst.inner.ptr_mut().deep_copy(self.inner.ptr(), dst.inner.arena()) }
        );
        dst
    }
}
impl<'msg> ::protobuf::IntoProxied<HealthCheckResponse>
for HealthCheckResponseMut<'msg> {
    fn into_proxied(
        self,
        _private: ::protobuf::__internal::Private,
    ) -> HealthCheckResponse {
        ::protobuf::IntoProxied::into_proxied(
            ::protobuf::IntoView::into_view(self),
            _private,
        )
    }
}
impl ::protobuf::__internal::runtime::EntityType for HealthCheckResponse {
    type Tag = ::protobuf::__internal::runtime::MessageTag;
}
impl<'msg> ::protobuf::__internal::runtime::EntityType
for HealthCheckResponseView<'msg> {
    type Tag = ::protobuf::__internal::runtime::ViewProxyTag;
}
impl<'msg> ::protobuf::__internal::runtime::EntityType for HealthCheckResponseMut<'msg> {
    type Tag = ::protobuf::__internal::runtime::MutProxyTag;
}
#[allow(dead_code)]
#[allow(non_camel_case_types)]
pub struct HealthCheckResponseMut<'msg> {
    inner: ::protobuf::__internal::runtime::MessageMutInner<'msg, HealthCheckResponse>,
}
impl<'msg> ::protobuf::__internal::SealedInternal for HealthCheckResponseMut<'msg> {}
impl<'msg> ::protobuf::MessageMut<'msg> for HealthCheckResponseMut<'msg> {
    type Message = HealthCheckResponse;
}
impl ::std::fmt::Debug for HealthCheckResponseMut<'_> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "{}", ::protobuf::__internal::runtime::debug_string(self))
    }
}
impl<
    'msg,
> From<::protobuf::__internal::runtime::MessageMutInner<'msg, HealthCheckResponse>>
for HealthCheckResponseMut<'msg> {
    fn from(
        inner: ::protobuf::__internal::runtime::MessageMutInner<
            'msg,
            HealthCheckResponse,
        >,
    ) -> Self {
        Self { inner }
    }
}
#[allow(dead_code)]
impl<'msg> HealthCheckResponseMut<'msg> {
    #[doc(hidden)]
    pub fn as_message_mut_inner(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessageMutInner<'msg, HealthCheckResponse> {
        self.inner
    }
    pub fn to_owned(&self) -> HealthCheckResponse {
        ::protobuf::AsView::as_view(self).to_owned()
    }
    pub fn status(&self) -> super::health_check_response::ServingStatus {
        unsafe {
            self.inner
                .ptr()
                .get_i32_at_index(
                    0,
                    (super::health_check_response::ServingStatus::Unknown).into(),
                )
                .try_into()
                .unwrap()
        }
    }
    pub fn set_status(&mut self, val: super::health_check_response::ServingStatus) {
        unsafe { self.inner.ptr_mut().set_base_field_i32_at_index(0, val.into()) }
    }
}
unsafe impl Send for HealthCheckResponseMut<'_> {}
unsafe impl Sync for HealthCheckResponseMut<'_> {}
impl<'msg> ::protobuf::AsView for HealthCheckResponseMut<'msg> {
    type Proxied = HealthCheckResponse;
    fn as_view(&self) -> ::protobuf::View<'_, HealthCheckResponse> {
        HealthCheckResponseView {
            inner: ::protobuf::__internal::runtime::MessageViewInner::view_of_mut(
                self.inner,
            ),
        }
    }
}
impl<'msg> ::protobuf::IntoView<'msg> for HealthCheckResponseMut<'msg> {
    fn into_view<'shorter>(self) -> ::protobuf::View<'shorter, HealthCheckResponse>
    where
        'msg: 'shorter,
    {
        HealthCheckResponseView {
            inner: ::protobuf::__internal::runtime::MessageViewInner::view_of_mut(
                self.inner,
            ),
        }
    }
}
impl<'msg> ::protobuf::AsMut for HealthCheckResponseMut<'msg> {
    type MutProxied = HealthCheckResponse;
    fn as_mut(&mut self) -> HealthCheckResponseMut<'msg> {
        HealthCheckResponseMut {
            inner: self.inner,
        }
    }
}
impl<'msg> ::protobuf::IntoMut<'msg> for HealthCheckResponseMut<'msg> {
    fn into_mut<'shorter>(self) -> HealthCheckResponseMut<'shorter>
    where
        'msg: 'shorter,
    {
        self
    }
}
#[allow(dead_code)]
impl HealthCheckResponse {
    pub fn new() -> Self {
        Self {
            inner: ::protobuf::__internal::runtime::OwnedMessageInner::<Self>::new(),
        }
    }
    #[doc(hidden)]
    pub fn as_message_mut_inner(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessageMutInner<'_, HealthCheckResponse> {
        ::protobuf::__internal::runtime::MessageMutInner::mut_of_owned(&mut self.inner)
    }
    pub fn as_view(&self) -> HealthCheckResponseView<'_> {
        ::protobuf::__internal::runtime::MessageViewInner::view_of_owned(&self.inner)
            .into()
    }
    pub fn as_mut(&mut self) -> HealthCheckResponseMut<'_> {
        ::protobuf::__internal::runtime::MessageMutInner::mut_of_owned(&mut self.inner)
            .into()
    }
    pub fn status(&self) -> super::health_check_response::ServingStatus {
        unsafe {
            self.inner
                .ptr()
                .get_i32_at_index(
                    0,
                    (super::health_check_response::ServingStatus::Unknown).into(),
                )
                .try_into()
                .unwrap()
        }
    }
    pub fn set_status(&mut self, val: super::health_check_response::ServingStatus) {
        unsafe { self.inner.ptr_mut().set_base_field_i32_at_index(0, val.into()) }
    }
}
impl ::std::ops::Drop for HealthCheckResponse {
    #[inline]
    fn drop(&mut self) {}
}
impl ::std::clone::Clone for HealthCheckResponse {
    fn clone(&self) -> Self {
        self.as_view().to_owned()
    }
}
impl ::protobuf::AsView for HealthCheckResponse {
    type Proxied = Self;
    fn as_view(&self) -> HealthCheckResponseView<'_> {
        self.as_view()
    }
}
impl ::protobuf::AsMut for HealthCheckResponse {
    type MutProxied = Self;
    fn as_mut(&mut self) -> HealthCheckResponseMut<'_> {
        self.as_mut()
    }
}
unsafe impl ::protobuf::__internal::runtime::AssociatedMiniTable
for HealthCheckResponse {
    fn mini_table() -> ::protobuf::__internal::runtime::MiniTablePtr {
        static ONCE_LOCK: ::std::sync::OnceLock<
            ::protobuf::__internal::runtime::MiniTableInitPtr,
        > = ::std::sync::OnceLock::new();
        unsafe {
            ONCE_LOCK
                .get_or_init(|| {
                    super::grpc__health__v1__HealthCheckResponse_msg_init.0 = ::protobuf::__internal::runtime::build_mini_table(
                        "$.P",
                    );
                    ::protobuf::__internal::runtime::link_mini_table(
                        super::grpc__health__v1__HealthCheckResponse_msg_init.0,
                        &[],
                        &[],
                    );
                    ::protobuf::__internal::runtime::MiniTableInitPtr(
                        super::grpc__health__v1__HealthCheckResponse_msg_init.0,
                    )
                })
                .0
        }
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetArena for HealthCheckResponse {
    fn get_arena(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> &::protobuf::__internal::runtime::Arena {
        self.inner.arena()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetMessagePtrMut
for HealthCheckResponse {
    type Msg = HealthCheckResponse;
    fn get_ptr_mut(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessagePtr<HealthCheckResponse> {
        self.inner.ptr_mut()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetMessagePtr for HealthCheckResponse {
    type Msg = HealthCheckResponse;
    fn get_ptr(
        &self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessagePtr<HealthCheckResponse> {
        self.inner.ptr()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetMessagePtrMut
for HealthCheckResponseMut<'_> {
    type Msg = HealthCheckResponse;
    fn get_ptr_mut(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessagePtr<HealthCheckResponse> {
        self.inner.ptr_mut()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetMessagePtr
for HealthCheckResponseMut<'_> {
    type Msg = HealthCheckResponse;
    fn get_ptr(
        &self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessagePtr<HealthCheckResponse> {
        self.inner.ptr()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetMessagePtr
for HealthCheckResponseView<'_> {
    type Msg = HealthCheckResponse;
    fn get_ptr(
        &self,
        _private: ::protobuf::__internal::Private,
    ) -> ::protobuf::__internal::runtime::MessagePtr<HealthCheckResponse> {
        self.inner.ptr()
    }
}
unsafe impl ::protobuf::__internal::runtime::UpbGetArena for HealthCheckResponseMut<'_> {
    fn get_arena(
        &mut self,
        _private: ::protobuf::__internal::Private,
    ) -> &::protobuf::__internal::runtime::Arena {
        self.inner.arena()
    }
}
pub mod health_check_response {
    #[repr(transparent)]
    #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
    pub struct ServingStatus(i32);
    #[allow(non_upper_case_globals)]
    impl ServingStatus {
        pub const Unknown: ServingStatus = ServingStatus(0);
        pub const Serving: ServingStatus = ServingStatus(1);
        pub const NotServing: ServingStatus = ServingStatus(2);
        pub const ServiceUnknown: ServingStatus = ServingStatus(3);
        fn constant_name(&self) -> ::std::option::Option<&'static str> {
            #[allow(unreachable_patterns)]
            Some(
                match self.0 {
                    0 => "Unknown",
                    1 => "Serving",
                    2 => "NotServing",
                    3 => "ServiceUnknown",
                    _ => return None,
                },
            )
        }
    }
    impl ::std::convert::From<ServingStatus> for i32 {
        fn from(val: ServingStatus) -> i32 {
            val.0
        }
    }
    impl ::std::convert::From<i32> for ServingStatus {
        fn from(val: i32) -> ServingStatus {
            Self(val)
        }
    }
    impl ::std::default::Default for ServingStatus {
        fn default() -> Self {
            Self(0)
        }
    }
    impl ::std::fmt::Debug for ServingStatus {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
            if let Some(constant_name) = self.constant_name() {
                write!(f, "ServingStatus::{}", constant_name)
            } else {
                write!(f, "ServingStatus::from({})", self.0)
            }
        }
    }
    impl ::protobuf::IntoProxied<i32> for ServingStatus {
        fn into_proxied(self, _: ::protobuf::__internal::Private) -> i32 {
            self.0
        }
    }
    impl ::protobuf::__internal::SealedInternal for ServingStatus {}
    impl ::protobuf::Proxied for ServingStatus {
        type View<'a> = ServingStatus;
    }
    impl ::protobuf::AsView for ServingStatus {
        type Proxied = ServingStatus;
        fn as_view(&self) -> ServingStatus {
            *self
        }
    }
    impl<'msg> ::protobuf::IntoView<'msg> for ServingStatus {
        fn into_view<'shorter>(self) -> ServingStatus
        where
            'msg: 'shorter,
        {
            self
        }
    }
    unsafe impl ::protobuf::__internal::Enum for ServingStatus {
        const NAME: &'static str = "ServingStatus";
        fn is_known(value: i32) -> bool {
            matches!(value, 0 | 1 | 2 | 3)
        }
    }
    impl ::protobuf::__internal::runtime::EntityType for ServingStatus {
        type Tag = ::protobuf::__internal::runtime::EnumTag;
    }
}
