# Example code from admin panel

@app.route('/facebook/leads', methods=['POST'])
def facebook_get_leads():
    logger.debug('start')
    request_body = json.loads(request.get_json())
    logger.debug(f'request.get_json(): {request_body}')
    request_body = json.loads(request_body.get('body'))
    facebook_lead_process(request_body=request_body, logger=logger, mode='real_time')
    return 'Facebook get lead process finished'


def facebook_lead_process(request_body, logger, mode='real_time'):
    """
        Функция парсит фейсбучный запрос, и распределяет лиды. Параметр mode нужен для того, чтобы была возможность
        отключать создание отложенных лидов и отключать отправку уведомлений в телеграм. Это нужно в случаях, когда
        фейсбукчный запрос берется не напрямую с вебхука, а с базы, с отложенных лидов.
    """
    logger.debug(f'type request_body - {type(request_body)}')
    page_id = request_body['entry'][0]['changes'][0]['value'].get('page_id')
    page = FacebookPage.query.filter_by(id=page_id).first()
    if not page:
        logger.debug(f'Lead from new Facebook page: {page_id}')
        return False
    access_token = page.token
    try:
        form_id = request_body['entry'][0]['changes'][0]['value'].get('form_id')
        logger.debug(f'form_id: {form_id}')
        ad_id = request_body['entry'][0]['changes'][0]['value'].get('ad_id')
        logger.debug(f'ad_id: {ad_id}')
        leadgen_id = request_body['entry'][0]['changes'][0]['value'].get('leadgen_id')
        logger.debug(f'leadgen_id: {leadgen_id}')
    except Exception:
        logger.debug(f'request_body structure changed {request_body}')
        return False
    if leadgen_id and form_id and page_id and access_token:
        data = get_lead_from_facebook(leadgen_id, access_token)

        if data:
            logger.debug(f'data: {data}')
            facebook_form = FacebookForm.query.filter_by(id=form_id).first()
            # Получаем список полей field_data с объекта лида и неизвестные системе поля, если они есть
            field_data = {item.get('name'): item.get('values') for item in data.get('field_data')}
            endpoint = field_data.get('endpoint') # если значение bitrix - лид уйдет в битрикс
            if endpoint:
                # Могут ошибиться со значением поля, по этому страхуемся
                endpoint = 'bitrix'
            else:
                endpoint = 'crm'
            unknown_fields = check_unknown_fields(field_data, endpoint)

            if not facebook_form:
                logger.debug(f'Нет лид формы, добавляем новую')
                new_form = add_new_lead_form(field_data, form_id, page, endpoint)
                if new_form:
                    logger.debug(f'Лид форма добавлена - {form_id}')
                    update_facebook_form_name.apply_async([FB_APP_ID, FB_APP_SECRET, access_token, form_id])
                if len(unknown_fields.items()) == 1 and 'endpoint' in unknown_fields.keys():
                    # Проверка на единственное неизвестное поле endpoint (если True - значит лид для bitrix)
                    lead = create_bitrix_lead_from_facebook_fields(data, field_data)
                    lead_id = b24.add_lead_to_crm(**lead.to_dict())
                    logger.debug(f'Добавляем лида в битрикс, так как длина unknown_fields равна 1 и это endpoint')
                    if lead_id.get('result'):
                        # Если успешно добавлен - добавляем published lead
                        create_published_lead(lead, lead_id.get('result'), new_form)
                        logger.debug(f'Лид в битрикс успешно добавлен. Добавили published lead')
                        return True
                    else:
                        logger.debug(f'Лид в битрикс не попал. Добавили delayed lead. Ответ от битрикса - {lead_id}')
                        if mode == 'real_time':
                            # Если ошибка при добавлении лида в битрикс, оповещаем, и добавляем delayed lead
                            create_delayed_lead(data=request_body, source='facebook')
                            techdep_bot_send_msg.apply_async([f'Ошибка при добавлении лида в битрикс. '
                                                              f'Форма - {new_form}'])
                        return False
                elif unknown_fields:
                    logger.debug(f'Добавлен delayed lead. '
                                 f'Есть неизвестные поля - {[key for key in unknown_fields.keys()]}')

                    if 'endpoint' in unknown_fields.keys():
                        unknown_fields.pop('endpoint')
                    # Если есть неизвестные поля
                    add_mapped_fields_to_db(unknown_fields)
                    if mode == 'real_time':
                        create_delayed_lead(data=request_body, source='facebook')
                        techdep_bot_send_msg.apply_async([f'Нужно сопоставить поля - '
                                                          f'{[key for key in unknown_fields.keys()]}. '
                                                          f'Добавлена новая лид форма {form_id}'])
                    return False
                elif new_form.school_name:
                    # На фейсбуке в форме был прописан филиал, добавляем лиды
                    lead = create_crm_lead_from_facebook_fields(data, field_data)
                    lead, crm = set_fb_lead_field_priority(page, new_form, lead)
                    lead_id, lead = add_lead_to_crm(lead, crm, logger)
                    if lead_id:
                        # Если лид добавлен в срм - добавляем published lead
                        logger.debug(f'Добавлен новый лид. Страна - {lead.country}, lead_id - {lead_id}')
                        create_published_lead(lead, lead_id, new_form)
                        return True
                    else:
                        logger.debug(f'Лид в срм не попал. Добавили delayed lead.')
                        if mode == 'real_time':
                            create_delayed_lead(data=request_body, source='facebook')
                            techdep_bot_send_msg.apply_async([f'Ошибка при добавлении лида в срм. '
                                                              f'Форма - {new_form}'])
                        return False

                if mode == 'real_time':
                    create_delayed_lead(data=request_body, source='facebook')
                    techdep_bot_send_msg.apply_async([f'Добавлена новая лид форма {form_id}. Добавлен отложеный лид'])
                return False

            elif facebook_form:
                unknown_fields = check_unknown_fields(field_data, facebook_form.endpoint)
                if unknown_fields:
                    logger.debug(f'Добавлен delayed lead. '
                                 f'Есть неизвестные поля - {[key for key in unknown_fields.keys()]}')
                    if 'endpoint' in unknown_fields.keys():
                        unknown_fields.pop('endpoint')
                    # Если есть неизвестные поля
                    add_mapped_fields_to_db(unknown_fields)
                    if mode == 'real_time':
                        create_delayed_lead(data=request_body, source='facebook')
                        techdep_bot_send_msg.apply_async([f'Нужно сопоставить поля - '
                                                          f'{[key for key in unknown_fields.keys()]}'])
                    return False
                if facebook_form.endpoint == 'crm':
                    if facebook_form.school_name:
                        # На фейсбуке в форме был прописан филиал, добавляем лиды
                        lead = create_crm_lead_from_facebook_fields(data, field_data)
                        lead, crm = set_fb_lead_field_priority(page, facebook_form, lead)
                        logger.debug(lead)
                        lead_id, lead = add_lead_to_crm(lead, crm, logger)
                        if lead_id:
                            # Если лид добавлен в срм - добавляем published lead
                            logger.debug(f'Добавлен новый лид. Страна - {lead.country}, lead_id - {lead_id}')
                            create_published_lead(lead, lead_id, facebook_form)
                            return True
                        else:
                            logger.debug(f'Лид в срм не попал. Добавили delayed lead.')
                            if mode == 'real_time':
                                create_delayed_lead(data=request_body, source='facebook')
                                techdep_bot_send_msg.apply_async([f'Ошибка при добавлении лида в срм. '
                                                                  f'Форма - {facebook_form}'])
                            return False

                    elif not facebook_form.school_name:
                        logger.debug(f'Добавлен delayed lead. К форме {facebook_form.form_name} не подвязана школа')
                        if mode == 'real_time':
                            create_delayed_lead(data=request_body, source='facebook')
                            techdep_bot_send_msg.apply_async([f'Появился новый лид для существующей формы '
                                                              f'{facebook_form.form_name}. Подвяжите школу к форме'])
                        return False

                elif facebook_form.endpoint == 'bitrix':
                    lead = create_bitrix_lead_from_facebook_fields(data, field_data)
                    lead_id = b24.add_lead_to_crm(**lead.to_dict())
                    logger.debug(f'Добавляем лида в битрикс')
                    if lead_id.get('result'):
                        # Если успешно добавлен - добавляем published lead
                        create_published_lead(lead, lead_id.get('result'), facebook_form)
                        logger.debug(f'Лид в битрикс успешно добавлен. Добавили published lead')
                        return True
                    else:
                        # Если ошибка при добавлении лида в битрикс, оповещаем, и добавляем delayed lead
                        logger.debug(f'Лид в битрикс не попал. Добавили delayed lead. Ответ от битрикса - {lead_id}')
                        if mode == 'real_time':
                            create_delayed_lead(data=request_body, source='facebook')
                            techdep_bot_send_msg.apply_async([f'Ошибка при добавлении лида в битрикс. '
                                                              f'Форма - {facebook_form}'])
                        return False

        else:
            logger.debug(f'Пустой словарь data: {data}')
    return False


def add_new_lead_form(form_field, form_id, page, endpoint):
    facebook_form = ''
    if endpoint == 'crm':
        description = form_field.get('description')
        if description:
            description = description[0]
        school = form_field.get('school_name')
        if school:
            school = Schools.query.filter_by(name=school[0]).first()
        facebook_form = FacebookForm(
            id=form_id,
            date=get_current_datetime(),
            country_name=page.country[0],
            school_name=school,
            description=description,
            endpoint='crm'
        )
        db.session.add(facebook_form)
        db.session.commit()
    elif endpoint == 'bitrix':
        comment = form_field.get('description')
        if comment:
            comment = comment[0]
        facebook_form = FacebookForm(
            id=form_id,
            date=get_current_datetime(),
            description=comment or None,
            endpoint='bitrix'
        )
        db.session.add(facebook_form)
        db.session.commit()
    return facebook_form


def check_unknown_fields(form_fields, endpoint):
    unknown_fields = {}
    for key, value in form_fields.items():
        mapped_field = MappedFormField.query.filter_by(fb_field=key).first()
        if not mapped_field:
            unknown_fields[key] = value
        if endpoint == 'bitrix':
            if mapped_field and not mapped_field.bitrix_field:
                unknown_fields[key] = value
        elif not endpoint or endpoint == 'crm':
            if mapped_field and not mapped_field.crm_field:
                unknown_fields[key] = value
    return unknown_fields


def add_mapped_fields_to_db(fields):
    for field in fields:
        try:
            mapped_field = MappedFormField.query.filter_by(fb_field=field).first()
            if not mapped_field:
                mapped_field = MappedFormField(fb_field=field)
                db.session.add(mapped_field)
                db.session.commit()
        except Exception:
            continue
    return True


def add_lead_to_crm(lead, crm, logger=None):
    if not lead.school_id:
        lead.school_id = crm.default_school_id
        lead.assignee_id = crm.default_manager_id
    response = crm.push_lead(lead)
    logger.debug(f'Внутри add_lead_to_crm. Ответ сервера на добавление лида - {response.json()}')
    if logger:
        logger.debug(f'response.status_code - {response.status_code}')
        logger.debug(f'response.json() - {response.json()}')
    if response.status_code != 200:
        if 'Unknown ad source' in response.json().get('Error'):
            lead.ad_source = None
            response = crm.push_lead(lead)
    lead_id = response.json().get('LeadId')
    if response.status_code == 200 and lead_id:
        return int(lead_id), lead
    return None, lead


def set_fb_lead_field_priority(page: FacebookPage, facebook_form: FacebookForm, lead):
    crm = ''
    if facebook_form.description:
        # Приоритетней комментарий из FacebookForm, чем с данных, которые приходят с фейсбука
        lead.comment = facebook_form.description

    if isinstance(lead, CRMLead): # Если лид пойдет в срм
        lead.country = page.country[0]
        if facebook_form.country:
            # Приоритетней страна, что указана в фейсбук форме. От страны зависит в какую срм уйдет лид
            lead.country = facebook_form.country
            crm = facebook_form.country.crm
            lead.status = json.loads(crm.lead_statuses).get("1") #первый стандартный статус в срм
        if facebook_form.school_name:
            school = Schools.query.filter_by(name=facebook_form.school_name).first()
            if not school:
                response = crm.get_offices(**{
                    'name': facebook_form.school_name
                })
                if response.json().get('Offices'):
                    lead.school_id = response.json().get('Offices')[0].get('Id')
            elif school and school.crm_id != lead.school_id:
                # Приоритетней школа, из FacebookForm, чем с данных, которые приходят с фейсбука
                lead.school_id = school.crm_id
    elif isinstance(lead, BitrixLead):
        pass
    return lead, crm


def create_crm_lead_from_facebook_fields(data, form_fields) -> CRMLead:
    name = form_fields.get('full_name')
    if name:
        name = name[0]
    else:
        name = form_fields.get('first_name')
        if name:
            name = name[0]

    phone = form_fields.get('phone')
    if phone:
        phone = phone[0]

    email = form_fields.get('email')
    if email:
        email = email[0]

    age = form_fields.get('age')
    if age:
        age = age[0]

    school = form_fields.get('school_name')
    if school:
        school = Schools.query.filter_by(name=school[0]).first()
        if school:
            school = school.crm_id

    description = form_fields.get('description')
    if description:
        description = description[0]

    utm_source = 'facebook'
    utm_medium = 'cpc'
    utm_campaign = data.get('campaign_name')
    utm_content = data.get('adset_name')
    utm_term = data.get('ad_name')

    lead = CRMLead(
        full_name=name or 'Name Unknown',
        age=age,
        school_id=school,
        ad_source='facebook',
        email=email,
        phone=phone,
        comment=description,
        utm=[
            {'Name': 'utm_campaign', 'Value': utm_campaign},
            {'Name': 'utm_content', 'Value': utm_content},
            {'Name': 'utm_medium', 'Value': utm_medium},
            {'Name': 'utm_source', 'Value': utm_source},
            {'Name': 'utm_term', 'Value': utm_term}
        ]
    )
    for key, value in form_fields.items():
        mapped_field = MappedFormField.query.filter_by(fb_field=key).first()
        if mapped_field and mapped_field.crm_field:
            if mapped_field.crm_field == 'school_name':
                school = Schools.query.filter_by(name=value[0]).first()
                if school:
                    lead.school_id = school.crm_id
            elif mapped_field.crm_field == 'description':
                description = form_fields.get(mapped_field.fb_field)
                if description:
                    description = description[0]
                    lead.comment = description
            else:
                setattr(lead, mapped_field.crm_field, value[0])
    return lead


def get_lead_from_facebook(leadgen_id, access_token):
    FacebookAdsApi.init(FB_APP_ID, FB_APP_SECRET, access_token)
    fb_lead = Lead(leadgen_id)
    fb_lead_fields = ['ad_name', 'adset_name', 'campaign_name', 'partner_name', 'campaign_id', 'ad_id',
                      'adset_id', 'form_id', 'id', 'field_data', 'created_time', 'is_organic']
    fb_lead_params = {}
    try:
        data = fb_lead.api_get(fields=fb_lead_fields, params=fb_lead_params)
    except FacebookRequestError as fb_error:
        logger.error(f'FacebookRequestError - leadgen_id:{leadgen_id}')
        logger.error(fb_error)
        return None
    except Exception as e:
        logger.error(f'Ошибка - {e}')
        return None
    return data


def create_delayed_lead(data, source):
    delayed_lead = DelayedLead(
        date=get_current_datetime(),
        source=source,
        body=json.dumps(data)
    )
    db.session.add(delayed_lead)
    db.session.commit()


def create_published_lead(lead, lead_crm_id, facebook_form=None):
    if isinstance(lead, CRMLead):
        published_lead = PublishedLead(
            crm_id=lead_crm_id,
            publish_date=get_current_datetime(),
            school_name=Schools.query.filter_by(crm_id=lead.school_id).first(),
            country=lead.country,
            name=f'{lead.first_name} {lead.last_name}',
            phone=lead.phone,
            email=lead.email
        )
        if facebook_form:
            published_lead.facebook_form_id = facebook_form.id
        db.session.add(published_lead)
    elif isinstance(lead, BitrixLead):
        published_lead = PublishedLead(
            crm_id=lead_crm_id,
            publish_date=get_current_datetime(),
            country='Ukraine',
            name=f'{lead.name} {lead.last_name}',
            phone=lead.phone,
            email=lead.email,
            endpoint='bitrix'
        )
        if facebook_form:
            published_lead.facebook_form_id = facebook_form.id
        db.session.add(published_lead)
    db.session.commit()
